#define _GNU_SOURCE

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "page.h"

#define OFFSET_TO_PAGE_ID(_offset) (((_offset) / page_size) - 1)
#define PAGE_ID_TO_OFFSET(_page_id) (((_page_id) + 1) * page_size)
#define RID(_page_id, _slot_id) (((uint64_t) _page_id << 32) | (_slot_id))

int page_size = 4096;
int record_size = 8;
int num_records = 1000;
int random_rid = 0;
int interval = 100;	
int num_delete = 10;	
uint32_t file_offset;

struct Table {
	int fd;
	uint32_t record_size;
	struct Page *last_data_page;
	struct Page *last_dir_page;
	struct Page *last_read_page;
};

struct Table *table = NULL;

int CreateTable(const char *filename, uint32_t record_size)
{
	int fd;
	struct Page *header_page;

	fd = open(filename, O_RDWR | O_CREAT | O_TRUNC | O_DIRECT, 0644);
	if (fd < 0)
		return -1;

	header_page = CreatePage(DIR_PAGE_TYPE,
				page_size, sizeof(struct page_record));
	WritePage(fd, header_page);

	table = malloc(sizeof(struct Table));
	table->fd = fd;
	file_offset = page_size;
	table->record_size = record_size;
	table->last_data_page = NULL;
	table->last_dir_page = header_page;
	table->last_read_page = NULL;

	return fd;
}

bool SyncFile(int fd)
{
	bool ret;

	if (table->last_data_page)
		ret = WritePage(fd, table->last_data_page);
	ret = WritePage(fd, table->last_dir_page);
	fsync(fd);
	return ret;
}

void CloseTable(int fd)
{
	SyncFile(fd);
	FreePage(table->last_data_page);
	FreePage(table->last_dir_page);
	FreePage(table->last_read_page);
	close(fd);
	free(table);
	table = NULL;
}

static bool isDataPage(uint32_t page_id)
{
	uint32_t max_data_pages_dir; 

	if (page_id == (uint32_t) -1)
		return false;

	max_data_pages_dir = (page_size - sizeof(struct PageHeader)) / sizeof(struct page_record);

	return (((page_id + 1) % max_data_pages_dir) != 0);
}

struct Page *getDataPage(int fd, uint32_t page_id)
{
	int offset;
	struct Page *page;

	if (!isDataPage(page_id))
		return NULL;

	offset = PAGE_ID_TO_OFFSET(page_id);

	if (offset >= file_offset)
		return NULL;

	if (table->last_read_page &&
		table->last_read_page->page_record.offset ==
			offset)
		return table->last_read_page;

	if (table->last_data_page &&
		table->last_data_page->page_record.offset ==
			offset)
		return table->last_data_page;

	page = ReadPage(table->fd, record_size,
			DATA_PAGE_TYPE, offset);
	if (page) {
		if (table->last_read_page)
			FreePage(table->last_read_page);
		table->last_read_page = page;
	}
	return page;
}

struct Page *InsertPage(int fd,
				uint32_t record_size, uint32_t type)
{
	struct Page *new_page;
	uint32_t max_records;
	uint32_t record_count;

	max_records = (page_size - sizeof(struct PageHeader)) /
					sizeof(struct page_record);
	record_count = table->last_dir_page->raw_page->header.record_count;

	if (type == DATA_PAGE_TYPE &&
		(record_count > max_records - 2)) {
		InsertPage(fd, sizeof(struct page_record),
					DIR_PAGE_TYPE);
	}

	new_page = CreatePage(type, page_size, record_size);
	file_offset += page_size;

	table->last_dir_page->Insert(table->last_dir_page,
				(const char *) &new_page->page_record);

	if (type == DIR_PAGE_TYPE) {
		LinkPage(table->last_dir_page, new_page);
		WritePage(table->fd, table->last_dir_page);
		FreePage(table->last_dir_page);
		table->last_dir_page = new_page;
	} else if (type == DATA_PAGE_TYPE) {
		if (table->last_data_page) {
			LinkPage(table->last_data_page, new_page);
			WritePage(fd, table->last_data_page);
			FreePage(table->last_data_page);
		}
		table->last_data_page = new_page;
		WritePage(fd, table->last_dir_page);
	}
	return new_page;
}

bool Insert(const char *record)
{
	struct Page *last_data_page;
	struct Page *new_data_page; 
	struct PageHeader *header;
	uint32_t max_page_records;
	bool ret;

	last_data_page = table->last_data_page;
	if (last_data_page) {
		header = &last_data_page->raw_page->header;
		max_page_records = (page_size - sizeof(struct PageHeader)) /
			header->record_size;
		if (header->record_count <
			max_page_records) {
			ret = InsertRecord(last_data_page, record);

			return ret;
		}
	}

	new_data_page = InsertPage(table->fd,
						table->record_size,
						DATA_PAGE_TYPE);
	ret = InsertRecord(new_data_page, record);

	fsync(table->fd);
	return ret;
}

bool Read(uint64_t rid, char *buf)
{
	uint32_t page_id = rid >> 32;
	struct Page *data_page;
	const char *record;
	bool ret = false;

	data_page = getDataPage(table->fd, page_id);
	if (data_page) {
		record = ReadRecord(data_page, rid);
		if (record) {
			memcpy(buf, record, table->record_size);
			ret = true;
		}
	}
	return ret;
}

static struct Page *getPrevPage(struct Page *page)
{
	int type = page->page_record.type;
	uint32_t offset = page->page_record.offset;
	uint32_t prev_offset;
	uint32_t max_data_pages_dir; 
	size_t page_record_size;

	if (type == DATA_PAGE_TYPE) {
		if (offset == 0)
			return NULL;

		prev_offset = offset - page_size;
		if (!isDataPage(prev_offset / page_size - 1))
			prev_offset -= page_size;
		if (prev_offset > offset)
			return NULL;
	} else {
		max_data_pages_dir = (page_size - sizeof(struct PageHeader)) / sizeof(struct page_record);
		prev_offset = PAGE_ID_TO_OFFSET(OFFSET_TO_PAGE_ID(offset) -
						max_data_pages_dir);
		if (prev_offset > offset)
			return NULL;
	}
	page_record_size = (type == DATA_PAGE_TYPE) ?
			record_size : sizeof(struct page_record);

	return ReadPage(table->fd, page_record_size,
			type, prev_offset);
}

bool DeleteLastPage(void)
{
	struct PageHeader *dir_header;
	struct Page *prev_data;
	struct Page *prev_dir;
	struct PageHeader *prev_header;

	dir_header = &table->last_dir_page->raw_page->header;
	file_offset -= page_size;

	DeleteLastRecord(table->last_dir_page);

	prev_data = getPrevPage(table->last_data_page);

	if (prev_data)
		prev_data->raw_page->header.next = 0;

	if (table->last_read_page == table->last_data_page)
			table->last_read_page = NULL;

	FreePage(table->last_data_page);
	table->last_data_page = prev_data;

	if ((dir_header->record_count == 0) &&
		(table->last_dir_page->page_record.offset != 0)) {
		prev_dir = getPrevPage(table->last_dir_page);
		file_offset -= page_size;

		DeleteLastRecord(prev_dir);
		prev_header = &prev_dir->raw_page->header;
		prev_header->next = 0;

		FreePage(table->last_dir_page);
		table->last_dir_page = prev_dir;
	}

	ftruncate(table->fd, file_offset);
	return true;
}

bool Delete(uint64_t rid)
{
	uint32_t page_id = rid >> 32;
	uint32_t slot_id = rid;
	struct Page *data_page;
	const char *last_record;
	struct PageHeader *last_header;
	uint64_t last_rid;

	/* When deleting a record, move the last record of
	 * the last page into its place. */

	if (table->last_data_page && (PAGE_ID_TO_OFFSET(page_id) ==
		table->last_data_page->page_record.offset))
		data_page = table->last_data_page;
	else
		data_page = getDataPage(table->fd, page_id);
	if (!data_page)
		return false;

	if (data_page->raw_page->header.record_count <=
		slot_id)
		return false;

	last_header = &table->last_data_page->raw_page->header;
	last_rid = RID(
			OFFSET_TO_PAGE_ID(table->last_data_page->page_record.offset),
			last_header->record_count - 1);

	if (rid != last_rid) {
		last_record = table->last_data_page->Read(table->last_data_page, last_rid);
		if (!last_record)
			return false;
		InsertRecordAt(data_page, slot_id, last_record);
	}

	if (data_page != table->last_data_page)
		WritePage(table->fd, data_page);

	DeleteLastRecord(table->last_data_page);
	if (last_header->record_count == 0)
		DeleteLastPage();

	fsync(table->fd);
	return true;
}

static const struct args {
	const char *arg;
	int req_val;
	int *value;
} valid_args[] = {
		{ "-p", 1, &page_size, }, /* page size */
		{ "-s", 1, &record_size, }, /* record size */
		{ "-i", 1, &num_records, }, /* insert */
		{ "-n", 1, &interval, },	/* interval */
		{ "-d", 1, &num_delete, },  /* delete */
		{ "-r", 0, &random_rid, }, /* random */
};

int parse_arg(int *pos, int argc, char **argv)
{
	int i;
	int val;
	char *end;

	for (i = 0; i < sizeof(valid_args) / sizeof(valid_args[0]); i++) {
		if (strcmp(valid_args[i].arg, argv[*pos]))
			continue;

		*pos += 1;
		if (valid_args[i].value) {
			if (valid_args[i].req_val) {
				if (argc < *pos + 1)
					return 0;

				val = strtoul(argv[*pos], &end, 0);
				if (val == 0)
					return 0;
				*pos += 1;
			} else
				val = 1;

			*valid_args[i].value = val;
		}
		return 1;
	}
	return 1;
}

int parse_args(int argc, char **argv)
{
	int pos = 1;

	while (pos < argc)
		if (!parse_arg(&pos, argc, argv))
			return 0;
	return 1;
}

int ReadSequence(uint64_t rid, int interval, char *record)
{
	int i;
	uint32_t page_id = rid >> 32;
	uint32_t slot_id = rid;
	uint32_t page_records;

	page_records = (page_size - sizeof(struct PageHeader)) / (sizeof(uint64_t) + record_size);

	if (!isDataPage(page_id))
		page_id++;
			
	for (i = 0; i < interval; i++) {
		rid = ((uint64_t) page_id << 32) | slot_id;

		memset(record, 0, record_size);
		Read(rid, record);
		printf("%d.%d: %s\n",
				(int) (rid >> 32), (int) slot_id,
				record);

		slot_id++;
		if (slot_id >= page_records) {
			slot_id = 0;
			page_id++;
			if (!isDataPage(page_id))
				page_id++;
		}
	}
	return i;
}

uint64_t getRandRID(int page_records, int num_records)
{
	uint32_t page_id;
	uint32_t slot_id;

	if (num_records >= page_records)
		page_id = rand()%(num_records/page_records);
	else
		page_id = 0;

	slot_id = rand()%page_records;
	return ((uint64_t) page_id << 32) | slot_id;
}

int main(int argc, char **argv)
{
	int i;
	int fd;
	int status;
	uint32_t page_records;
	uint64_t rid;
	char *record;
	struct timespec start_insert, end_insert;
	struct timespec start_read_seq, end_read_seq;
	struct timespec start_read_rand, end_read_rand;
	time_t sec_delta;
	long nsec_delta;

	if (!parse_args(argc, argv)) {
		printf("Bad arguments\n");
		exit(1);
	}

	if ((page_size < 512) || (page_size & (page_size - 1))) {
		printf("Bad page size %d\n", page_size);
		exit(1);
	}

	fd = CreateTable("test_db.bin", record_size);

	record = calloc(1, record_size);

	clock_gettime(CLOCK_REALTIME, &start_insert);
	for (i = 0; i < num_records; i++) {
		snprintf(record, record_size - 1, "%d", i);

		status = Insert(record);
		if (!status) {
			printf("Insert failed at %d %d\n", i, status);
			break;
		}
		if (!(i % 10000))
			printf("Inserted %d\n", i);
	}
	SyncFile(table->fd);
	clock_gettime(CLOCK_REALTIME, &end_insert);

	sec_delta = end_insert.tv_sec - start_insert.tv_sec;
	nsec_delta = end_insert.tv_nsec - start_insert.tv_nsec;
	if (nsec_delta < 0) {
		nsec_delta += 1000000000;
		sec_delta--;
	}
	printf("Insert %d records took %ld sec %ld nsec\n", i, sec_delta, nsec_delta);

	page_records = (page_size - sizeof(struct PageHeader)) / (sizeof(uint64_t) + record_size);
	srand(time(NULL));

	if (!random_rid) {

		/* Read a sequence starting from a randomly-generated RID */
		rid = getRandRID(page_records, num_records);
		clock_gettime(CLOCK_REALTIME, &start_read_seq);
		i = ReadSequence(rid, interval, record);
		clock_gettime(CLOCK_REALTIME, &end_read_seq);

		sec_delta = end_read_seq.tv_sec - start_read_seq.tv_sec;
		nsec_delta = end_read_seq.tv_nsec - start_read_seq.tv_nsec;
		if (nsec_delta < 0) {
			nsec_delta += 1000000000;
			sec_delta--;
		}
		printf("Sequential read of %d records took %ld sec %ld nsec\n", i, sec_delta, nsec_delta);
	} else {
		/* Read an interval number of random RIDs */
		clock_gettime(CLOCK_REALTIME, &start_read_rand);
		for (i = 0; i < interval; i++) {
			rid = getRandRID(page_records, num_records);
			ReadSequence(rid, 1, record);
		}
		clock_gettime(CLOCK_REALTIME, &end_read_rand);

		sec_delta = end_read_rand.tv_sec - start_read_rand.tv_sec;
		nsec_delta = end_read_rand.tv_nsec - start_read_rand.tv_nsec;
		if (nsec_delta < 0) {
			nsec_delta += 1000000000;
			sec_delta--;
		}
		printf("Random read of %d records took %ld sec %ld nsec\n", i, sec_delta, nsec_delta);
	}

	// delete

	clock_gettime(CLOCK_REALTIME, &start_read_rand);
	for (i = 0; i < num_delete; i++) {
		rid = getRandRID(page_records, num_records - i);
		Delete(rid);
	}
	SyncFile(table->fd);
	clock_gettime(CLOCK_REALTIME, &end_read_rand);

	sec_delta = end_read_rand.tv_sec - start_read_rand.tv_sec;
	nsec_delta = end_read_rand.tv_nsec - start_read_rand.tv_nsec;
	if (nsec_delta < 0) {
		nsec_delta += 1000000000;
		sec_delta--;
	}
	printf("Random delete of %d records took %ld sec %ld nsec\n", i, sec_delta, nsec_delta);

	free(record);
	CloseTable(fd);
	return 0;
}
