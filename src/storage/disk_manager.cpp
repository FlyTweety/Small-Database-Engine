#include <stdexcept>
#include <sys/stat.h>

#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {

  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}


page_id_t DiskManager::AllocatePage() {//�Ӵ����з���һ������ҳ�������ؿ���ҳ���߼�ҳ��
  uint32_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  int notfull = 0;
  uint32_t extent_num = meta_page->num_extents_;
  uint32_t extent_id;
  for (extent_id = 0; extent_id < extent_num; extent_id++) {
    if (meta_page->extent_used_page_[extent_id] < PageNum) {
      notfull = 1;
      break;
    }
  }
  
  uint32_t page_offset;
  meta_page->num_allocated_pages_++;
  if (notfull) {//extend���½�page
    char bitmap_data[PAGE_SIZE];
    memset(bitmap_data, 0, PAGE_SIZE);
    ReadPhysicalPage((PageNum+1)*extent_id+1, bitmap_data);//����������ҳ���ݵĶ�ȡ
    (reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap_data))->AllocatePage(page_offset);
    meta_page->extent_used_page_[extent_id]++;
    WritePhysicalPage((PageNum+1)*extent_id+1, bitmap_data); //������ҳд�ش���
  }
  else {//�½�һ��extend
    meta_page->num_extents_++;
    meta_page->extent_used_page_[extent_id] = 1;
    BitmapPage<PAGE_SIZE>*bitmap = new BitmapPage<PAGE_SIZE>;
    bitmap->AllocatePage(page_offset);
    char *bitmap_data = reinterpret_cast<char *>(bitmap);
    WritePhysicalPage((PageNum+1)*extent_id+1, bitmap_data);
  }

  uint32_t logical_page_id = extent_id * PageNum + page_offset;//�����߼�ҳ��
  return logical_page_id;
}


void DiskManager::DeAllocatePage(page_id_t logical_page_id) {//�ͷŴ������߼�ҳ�Ŷ�Ӧ������ҳ��
  uint32_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  char bitmap[PAGE_SIZE];
  memset(bitmap, 0, PAGE_SIZE);

  uint32_t extent_id = logical_page_id / BITMAP_SIZE;//�������ҳ��
  uint32_t offset = logical_page_id % BITMAP_SIZE;//����ƫ����
  ReadPhysicalPage((PageNum+1)*extent_id+1, bitmap);//����������ҳ���ݵĶ�ȡ
  if ((reinterpret_cast<BitmapPage<PAGE_SIZE>*>(bitmap))->DeAllocatePage(offset)==false){//ɾ����ҳ����Ӧ������ҳ
    return;
  }
  meta_page->num_allocated_pages_ -= 1;
  meta_page->extent_used_page_[extent_id] -= 1;//��������
  WritePhysicalPage((PageNum+1)*extent_id+1, bitmap);//����������ҳ���ݵ�д��
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {//�жϸ��߼�ҳ�Ŷ�Ӧ������ҳ�Ƿ���С�
  uint32_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  char bitmap_data[PAGE_SIZE];
  memset(bitmap_data, 0, PAGE_SIZE);

  int32_t extent_id = logical_page_id / PageNum;//�������ҳ��
  ReadPhysicalPage((PageNum+1)*extent_id+1, bitmap_data);//����������ҳ���ݵĶ�ȡ
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
  uint32_t page_offset = logical_page_id % PageNum;
  if (bitmap->IsPageFree(page_offset) == true) return true;//�ж�����ҳ�Ƿ���С�
  else return false;

}


page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {//û�õ�
  uint32_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  return logical_page_id+logical_page_id/PageNum+2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
