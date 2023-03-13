#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"

//构造函�?
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(int index_in_page,
    B_PLUS_TREE_LEAF_PAGE_TYPE *current_page, BufferPoolManager *buffer_pool_manager) {
  index_in_page_ = index_in_page;
  current_page_ = current_page;
  buffer_pool_manager_ = buffer_pool_manager;

}

//析构函数
INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  if (current_page_ != nullptr) {
    buffer_pool_manager_->UnpinPage(current_page_->GetPageId(), false); //令人窒息的Unpin
  }
}

//访问当前迭代器所在位置的
INDEX_TEMPLATE_ARGUMENTS const MappingType &INDEXITERATOR_TYPE::operator*() {
  if(index_in_page_<0 || index_in_page_ > current_page_->GetSize()){
    std::cout<<"迭代器越界访�?"<<endl;
    throw std::exception();
    
  }
  return current_page_->GetItem(index_in_page_);  
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(index_in_page_ == current_page_->GetSize()-1){//当前已经到了该页中最后一�?
    if(current_page_->GetNextPageId() != INVALID_PAGE_ID){ //还有下一页，那就取到下一�?
      page_id_t next_page_id = current_page_->GetNextPageId();
      //Unpin
      buffer_pool_manager_->UnpinPage(current_page_->GetPageId(), false);
      auto* next_page = buffer_pool_manager_->FetchPage(next_page_id);
      current_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(next_page->GetData());
      assert(current_page_ != nullptr);
      index_in_page_ = 0;
    }
    else{ //越界怎么办？？要不要返回开�??????
      std::cout<<"迭代器越界末�?"<<endl;
      //没有抛出异常
    }
  }
  else{ //正常往后一个去�?
    index_in_page_++;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  if(itr.current_page_ == this->current_page_ && itr.index_in_page_ == this->index_in_page_){
    return true;
  }
  else return false;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  if(itr.current_page_ == this->current_page_ && itr.index_in_page_ == this->index_in_page_){
    return false;
  }
  else return true;
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
