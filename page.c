#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "page.h"

static void InitPage(struct Page *page, int type, uint32_t offset)
{
	page->Insert = InsertRecord;
	page->Read = ReadRecord;
	page->page_record.type = type;
	page->page_record.offset = offset;
	page->page_id = offset > 0 ?
				(offset - page_size) / page_size : 0;
}

static void InitRawPage(struct Page *page, uint32_t page_size,
				uint32_t record_size)
{
	struct PageHeader *header = &page->raw_page->header;

	header->page_size = page_size;
	header->record_size = record_size;
	if (page->page_record.type == DATA_PAGE_TYPE)
		header->record_size += sizeof(uint64_t);
	header->record_count = 0;
	header->next = 0;
}

static struct Page *AllocPage(int type,
					uint32_t page_size, uint32_t record_size, uint32_t offset)
{
	struct Page *new_page = NULL;
	char *raw_page = NULL;

	raw_page = calloc(1, page_size + BLOCK_SIZE);
	if (!raw_page)
		return NULL;

	new_page = malloc(sizeof(struct Page));
	new_page->non_aligned_page = raw_page;
	new_page->raw_page = (struct RawPage *) (((size_t) (raw_page + BLOCK_SIZE)) &
						~(BLOCK_SIZE - 1));

	InitPage(new_page, type, offset);
	return new_page;
}

struct Page *CreatePage(int type,
					uint32_t page_size, uint32_t record_size)
{
	struct Page *new_page;

	new_page = AllocPage(type, page_size, record_size, file_offset);
	InitRawPage(new_page, page_size, record_size);
	return new_page;
}

void FreePage(struct Page *page)
{
	if (!page)
		return;

	free(page->non_aligned_page);
	free(page);
}

struct Page *ReadPage(int fd, uint32_t record_size,
				uint32_t type, uint32_t offset)
{
	struct Page *new_page;

	new_page = AllocPage(type, page_size, record_size, offset);

	lseek(fd, offset, SEEK_SET);
	read(fd, new_page->raw_page, page_size);
	return new_page;
}

void LinkPage(struct Page *page, struct Page *next)
{
	page->raw_page->header.next = next->page_record.offset;
}

uint32_t GetNextOffset(struct Page *page)
{
	return page->raw_page->header.next;
}

static char *GetSlot(struct Page *page, uint32_t slot_id)
{
	struct PageHeader *header = &page->raw_page->header;
	char *slot = (char *) &header[1] +
				slot_id * header->record_size;
	return slot;
}

bool InsertRecordAt(struct Page *page, uint32_t slot_id,
					const char *record)
{
	struct PageHeader *header = &page->raw_page->header;
	uint32_t max_records;
	char *slot;

	max_records = (header->page_size - sizeof(*header)) / header->record_size;
	if (slot_id >= max_records)
		return false;

	slot = GetSlot(page, slot_id);

	if (page->page_record.type == DATA_PAGE_TYPE) {
		*(uint64_t *) slot = (uint64_t) page->page_id << 32 | slot_id;
		memcpy(slot + sizeof(uint64_t), record, header->record_size - sizeof(uint64_t));
	} else {
		memcpy(slot, record, header->record_size);
	}

	return true;

}

bool InsertRecord(struct Page *page, const char *record)
{
	struct PageHeader *header = &page->raw_page->header;
	bool status;

	status = InsertRecordAt(page, header->record_count, record);
	if (status)
		header->record_count++;
	return status;
}

const char *ReadRecord(struct Page *page, uint64_t rid)
{
	struct PageHeader *header = &page->raw_page->header;
	uint32_t slot_id = (uint32_t) rid;
	uint32_t page_id = rid >> 32;
	char *slot;

	if (page_id != page->page_id)
		return NULL;
	if (slot_id >= header->record_count)
		return NULL;

	slot = GetSlot(page, slot_id);
	slot += (page->page_record.type == DATA_PAGE_TYPE) ?
			sizeof(uint64_t) : 0;
	return slot;
}

bool DeleteLastRecord(struct Page *page)
{
	struct PageHeader *header = &page->raw_page->header;
	char *slot;

	if (header->record_count == 0)
		return false;

	slot = GetSlot(page, --header->record_count);
	memset(slot, 0, header->record_size);
	return true;
}

bool WritePage(int fd, struct Page *page)
{
	lseek(fd, page->page_record.offset, SEEK_SET);
	write(fd, page->raw_page, page_size);
	return true;
}

#if 0
#define page_size 4096
#define ARRAY_SIZE(a) (sizeof((a)) / sizeof((a)[0]))

struct record_type {
	uint32_t id;
	char name[64];
} records[] = {
{ .id = 1, .name = "test1", },
{ .id = 2, .name = "test2", },
{ .id = 3, .name = "test3", }
};

int main(void)
{
	struct Page *page1 = CreatePage(page_size, sizeof(struct record_type));
	uint32_t rid;
	const struct record_type *read;
	int i;

	for (i = 0; i < ARRAY_SIZE(records); i++)
		InsertRecord(page1, (char *) &records[i]);

	for (rid = 0; read = (const struct record_type *) ReadRecord(page1, (uint64_t) rid); rid++) {
		if (!read)
			break;

		printf("id: %d name: %s\n", read->id, read->name);
	}

	return 0;
}
#endif
