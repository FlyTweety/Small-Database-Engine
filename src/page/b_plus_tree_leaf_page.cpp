#include <algorithm>
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"//自己加的。可是目前还是和我说找不到内部节点这种类型 这搞P

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
//初始化函数
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetSize(0);
  SetMaxSize(max_size-1);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
//返回下一页的page_id
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

//设置下一页的page_id
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
//找到第一个array_[i].first >= key的下标
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const {
  //先看看是不是空的
  if(GetSize() == 0) return 0;
  //碰运气第一个是不是
  if(comparator(array_[0].first, key) >= 0) return 0;
  //修正 加上对最后一个的比对
  //如果等于最后一个，返回size - 1
  //如果比最后一个还大，返回size
  if(comparator(key, array_[GetSize()-1].first) == 0) return GetSize()-1;
  if(comparator(key, array_[GetSize()-1].first) > 0) return GetSize();
  //size>=10用二分，size<10个用顺序
  if(GetSize() > 10){ //二分查找
  //目前的问题是left到不了right+1 也就是最后一个
    int left = 1, right = GetSize()-1, mid;
    while(left < right){
      mid = (left + right) / 2;
      if (comparator(key, array_[mid].first) > 0) left = mid + 1;
      else right = mid;    
    }
    return left;
  }
  else{ //顺序查找 
    for(int i = 1; i < GetSize(); i++){
      if (comparator(key, array_[i].first) <= 0) {
        return i;
      }
    }
  }
  return GetSize();//说明都比key小
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
//返回index下标的key
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
//返回index下标的一对
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array_[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
//在叶子节点中插入一对值
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) {
  //找到插入的位置
  int index = KeyIndex(key, comparator);
  //如果已经有相同的就不再插入
  if (index < GetSize() && comparator(key, KeyAt(index))==0) {
    return GetSize();
  }
  //正常插入，后面的往后挪，赋值index位置
  for (int i = GetSize(); i > index; i--) {
        array_[i] = array_[i - 1];
  }
  array_[index] = std::make_pair(key, value);//瞎写的
  //调整大小
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
//把一半对值移到recipient
//这里为了split里使用使得和内部节点的定义完全一样，手动加上了传buffer_pool_manager，但后来又没用上
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(BPlusTreeLeafPage *recipient, BufferPoolManager *buffer_pool_manager) { 
  int half = GetSize()/2;
  recipient->CopyNFrom(array_ + GetSize() - half, half);
  SetSize(GetSize() - half);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
//获取size对值到自己
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyNFrom(MappingType *items, int size) {
  int start = GetSize(); //理论上是0个开始，我这里如果前面非0就在后面加（应该不会）
  for(int i = 0; i < size; i++){
    array_[i+start] = *items;
    items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
//在叶子节点中查找key是否存在，存在的话把value存到value然后返回true，否则返回false
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value, const KeyComparator &comparator) const {
  //先检查特殊情况
  if(GetSize()==0) return false;
  if(comparator(key,KeyAt(0)) < 0) return false;
  if(comparator(key,KeyAt(GetSize()-1)) > 0) return false;
  //查找
  if(GetSize() <= 10){//如果size<=10，顺序查找
    int is_found = 0;
    int index;
    for(int i = 0; i < GetSize(); i++){
      if(comparator(key,KeyAt(i)) == 0){
        is_found = 1;
        index = i;
        break;
      }
    }
    if(is_found == 1){
      value = array_[index].second;
      return true;
    }
    else return false;
  }
  else if(GetSize() > 10){//size>=10，二分查找
  //突然发现可以用key_index，我擦，顺序白写了
    int index = KeyIndex(key, comparator);
    if (index < GetSize() && comparator(key, KeyAt(index))==0){//找到了
      value = array_[index].second;
      return true;
    }
    else return false;
  }
  return false; //这行其实没用
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
//删除目标key。如果找到了目标key就删除然后后面往前，没找到就直接返回size
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(const KeyType &key, const KeyComparator &comparator, BufferPoolManager *buffer_pool_manager) {
  int index = KeyIndex(key, comparator);
  if (index < GetSize() && comparator(key, KeyAt(index))==0){//找到了

    for (int i = index + 1; i < GetSize(); ++i) {
      array_[i - 1] = array_[i];
    }
    //SetSize(GetSize()-1);
    IncreaseSize(-1);
  }
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
//把这个节点的值全部挪到recipient，也要更新recipient兄弟节点（相当于删除这个，全部放到左边去）
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  recipient->CopyNFrom(array_, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetSize(0);//虽然不知道有没有用但还是搞一下
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
//把这个节点的第一个借给左边，要更新父母节点的指针
//欸怎么没传buffer_pool_manager，更新不了父母啊
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key, BufferPoolManager *buffer_pool_manager) {
  //recipient接收
  recipient->array_[recipient->GetSize()] = this->array_[0];
  recipient->IncreaseSize(1);
  //this调整
  for(int i = 0; i < GetSize() - 1; i++){ //i < GetSize()?
    array_[i] = array_[i+1];
  }
  this->IncreaseSize(-1);
  //找到在父母节点中的位置，更新该位置key
  //这步放到外层做
  /*
  auto *page = buffer_pool_manager->FetchPage(this->GetParentPageId());
  auto *parent_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(page->GetData());
  int index_in_parent = parent_page->ValueIndex(this->GetPageId());
  parent_page->SetKeyAt(index_in_parent, this->KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);*/
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
//同内部节点一样，这个函数不需要，集成到上面了
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
//把这个节点的最后一个借给右边当第一个
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeLeafPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  //recipient接收
  for(int i = recipient->GetSize(); i > 0; i--){
    recipient->array_[i] = recipient->array_[i-1];
  }
  recipient->array_[0] = this->array_[this->GetSize()-1];
  recipient->IncreaseSize(1);
  //this调整
  this->IncreaseSize(-1);
  //找到recipient在父母节点中的位置，更新该位置key
  //这步放到外层做
  /*
  auto *page = buffer_pool_manager->FetchPage(recipient->GetParentPageId());
  auto *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  int index_in_parent = parent_page->ValueIndex(recipient->GetPageId());
  parent_page->SetKeyAt(index_in_parent, recipient->KeyAt(0));
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);*/
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
//同内部节点一样，这个函数不需要，集成到上面了
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(const MappingType &item) {
}

template
class BPlusTreeLeafPage<int, int, BasicComparator<int>>;

template
class BPlusTreeLeafPage<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTreeLeafPage<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTreeLeafPage<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTreeLeafPage<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTreeLeafPage<GenericKey<64>, RowId, GenericComparator<64>>;