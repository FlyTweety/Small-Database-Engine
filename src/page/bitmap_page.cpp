#include "page/bitmap_page.h"

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
	page_offset = next_free_page_;
	uint32_t byte_index = page_offset / 8;
	if(byte_index >= MAX_CHARS){
		return false;
	}
	uint32_t bit_index = page_offset % 8;
	unsigned char B = (0x01) << bit_index;
	bytes[byte_index] = bytes[byte_index] | B;
	page_allocated_++;
	if(page_allocated_/8 == MAX_CHARS){
		next_free_page_ = page_allocated_;
		return true;
	}
	while(!IsPageFree(next_free_page_)){
		next_free_page_++;
		if(next_free_page_ / 8 >= MAX_CHARS){
			return false;
		}
	}
	return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
	uint32_t byte_index = page_offset / 8;
	if(byte_index >= MAX_CHARS){
		return false;
	}
    uint32_t bit_index = page_offset % 8;
	unsigned char B = (0x01) << bit_index;
	if((B & bytes[byte_index]) != 0){
	}
	else{
		return false;
	}
	B = ~B;
	bytes[byte_index] = bytes[byte_index] & B;

	if(next_free_page_ > page_offset){
		next_free_page_ = page_offset;
	}
	page_allocated_--;
	return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
    uint32_t byte_index = page_offset / 8;
	if(byte_index >= MAX_CHARS){
		return false;
	}
    uint32_t bit_index = page_offset % 8;
    return IsPageFreeLow(byte_index, bit_index);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
    unsigned char B = bytes[byte_index];
	int result;
    B = B >> bit_index;
	result = B & 0x01;
	if(result){
		return false;
	}
	else{
		return true;
	}
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;
