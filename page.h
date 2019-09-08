#ifndef __PAGE_H_
#define __PAGE_H_

#include <stdbool.h>
#include <stdint.h>

#define BLOCK_SIZE 512
#define DATA_PAGE_TYPE 1
#define DIR_PAGE_TYPE 2

extern int page_size;
extern int record_size;
extern uint32_t file_offset;

struct PageHeader {
	uint32_t page_size;		// Total size of the page, including header
	uint32_t record_size;	// Size of each record
	uint32_t record_count;	// Number of records stored in the page
	uint32_t next;			// The next page in the same file
};

struct RawPage {
	struct PageHeader header;
	char data[];
};

struct page_record {
	uint32_t type;
	uint32_t offset;
};

struct Page {
	bool (*Insert)(struct Page *page, const char *record);
	const char * (*Read)(struct Page *page, uint64_t rid);

	char *non_aligned_page;
	struct RawPage *raw_page;
	struct page_record page_record;
	uint32_t page_id;
};

struct Page *CreatePage(int type,
					uint32_t page_size, uint32_t record_size);

bool InsertRecord(struct Page *page, const char *record);
const char *ReadRecord(struct Page *page, uint64_t rid);

void FreePage(struct Page *page);
uint32_t GetNextOffset(struct Page *page);
void LinkPage(struct Page *page, struct Page *next);
bool WritePage(int fd, struct Page *page);

struct Page *ReadPage(int fd, uint32_t record_size,
				uint32_t type, uint32_t offset);

bool DeleteLastRecord(struct Page *page);
bool InsertRecordAt(struct Page *page, uint32_t slot_id,
					const char *record);

#endif /* __PAGE_H_ */

