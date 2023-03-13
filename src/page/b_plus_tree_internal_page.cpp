#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) { //就把三个对应属性赋成三个对应的值
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0); 
  SetPageType(IndexPageType::INTERNAL_PAGE);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
//其实是这些都没做越界的检查
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const { 
  // replace with your own code
  KeyType key = array_[index].first;
  return key;
}

//设置指定下标的key
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array_[index].first = key;
}

//自己加的
//查找对应Key所在的下标
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyIndex(const KeyType &key, const KeyComparator &comparator) const { 
  //可以向二分优化
  for(int i = 0; i < GetSize(); i++){
    if(comparator(array_[i].first, key) == 0){
      return i;
    }
  }
  //不知道下面这个return会有什么影响
  return 0;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
//查找对应value所在的下标
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const { 
  //可以向二分优化
  for(int i = 0; i < GetSize(); i++){
    if(array_[i].second == value){
      return i;
    }
  }
  //不知道下面这个return会有什么影响
  return 0;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
//返回指定下标的value
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // replace with your own code
  ValueType val = array_[index].second;
  return val;
}

//为啥key有set，value没有set啊喂
//自己整个set value吧
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
   array_[index].second = value;
}


/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
//给定key，返回孩子节点指针
//这个一定要找到吗？
//因为可能是没有找到的我擦，处于两个之间
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
//二分查找
  int start = 1, end = GetSize();
    while (start < end) {
        int mid = start + (end - start) / 2;
        if (comparator(key, KeyAt(mid)) < 0) {
          end = mid;
        } 
        else {
          start = mid + 1;
        }
    }
    return ValueAt(start - 1);
  
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
//创建一个新内部节点其实是
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  this->array_[0].second = old_value;
  SetKeyAt(1, new_key);
  this->array_[1].second = new_value;
  SetSize(2); 
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */ 
//这个就是在那个数组里合适位置插入一对，然后后面的往后挪一个。返回插入后size的大小
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {               
  //这里没有传比较器，要用前文的ValueIndex()函数
  int target_index = ValueIndex(old_value);
  SetSize(GetSize() + 1); //size++
  //后挪一个
  for(int i = GetSize() - 1; i > target_index + 1; i--){ 
    array_[i] = array_[i-1];
  }
  //然后在空出的位置插入新的
  array_[target_index+1].first = new_key;
  array_[target_index+1].second = new_value;
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
//把本页的一半挪到新页去
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  //把后一半挪到新页 
  for(int i = (GetSize()+1)/2; i < GetSize(); i++){ //假设getsize = 5，这里i=3开始，把34移过去，后面也是对的
    recipient->array_[i-((GetSize()+1)/2)] = this->array_[i];
  }
  recipient->SetSize(GetSize()-((GetSize()+1)/2));
  this->SetSize((GetSize()+1)/2);
  //改变孩子页面的父母page_id
  for(int i = 0; i < recipient->GetSize(); i++){
    auto *page = buffer_pool_manager->FetchPage(recipient->ValueAt(i)); //要加recipient 否则变成this了
    if (page != nullptr) { //没有做检查如果空可咋办
      auto *child_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      child_node->SetParentPageId(recipient->GetPageId());
      buffer_pool_manager->UnpinPage(recipient->ValueAt(i), true);
    }
  }
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
//复制N个条目到我这个页面
//同时要改写孩子们父母页面的ID到我
//这个好像就是上面那个？？？或者说上面那个应该用这个写
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  //初始可以不为空
  int start = GetSize(); //如果Init页面时size不是0的话，这个就挂了
  for(int i = 0; i < size; i++){
    array_[i + start] = *items;
    items++;
  }
  SetSize(GetSize() + size);
  //调整孩子页面的父母到这里来
  for(int i = start; i < GetSize(); i++){ //从start开始
    auto *page = buffer_pool_manager->FetchPage(ValueAt(i));
    if (page != nullptr) { //没有做检查如果空可咋办
      auto *child_node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      child_node->SetParentPageId(GetPageId());
      buffer_pool_manager->UnpinPage(ValueAt(i), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
//删掉特定位置的，把后面往前挪一个
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  //往前挪
  for(int i = index; i < GetSize() - 1; i++){
    array_[i] = array_[i+1];
  }
  //调整size
  SetSize(GetSize() - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
//目前写的就是把array_里第一个给返回
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  // replace with your own code
  ValueType val = ValueAt(0);
  SetSize(0); //因为size从0开始
  return val;
}

/*****************************************************************************
 * MERGE 
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  array_[0].first = middle_key; //这样是不对的，但是方便下一步
  //然后现在可以愉快地直接在recipient后面插入
  for(int i = 0; i < GetSize(); i++){
    recipient->array_[recipient->GetSize()+i] = array_[i]; //整对直接搬，0的key也是有效
    auto *page = buffer_pool_manager->FetchPage(ValueAt(i));
    auto *child_page = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData()); //插入同时改child
    child_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  recipient->IncreaseSize(this->GetSize()); //调整size
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE 
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
//自己是右边，把第一个借给左边
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {         
  //另一种组成pair的方式
  auto *page = buffer_pool_manager->FetchPage(recipient->GetParentPageId()); //接受页和该页的父母应当是一样的
  auto *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  int index_in_parent = parent_page->ValueIndex(GetPageId());
  MappingType pair = {parent_page->KeyAt(index_in_parent), ValueAt(0)}; //从父母里找到孩子的键值（自己没有），加上孩子的指针

  //调整父母页
  parent_page->SetKeyAt(index_in_parent, KeyAt(1));
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);

  //调整接受页(默认要删的已删除)
  //这步好像成功
  recipient->array_[recipient->GetSize()] = pair;
  recipient->IncreaseSize(1);

  //调整自己页
  //这个也有问题
  SetValueAt(0, ValueAt(1));
  Remove(1); //不能remove(0)我擦
  
  //调整孩子页
  auto *another_page = buffer_pool_manager->FetchPage(recipient->ValueAt(recipient->GetSize() - 1)); 
  auto child_page = reinterpret_cast<BPlusTreePage *>(another_page->GetData());
  child_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
//在末尾接收一条，和上面MoveFirstToEndOf配合
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  //没有用到，被集成到上面了
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient //意思可能是挪了一个孩子节点，它的父亲也要移到现在
 */
//向左边借一个，往右边去一对条目，和下面CopyFirstFrom配合
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  //【找到pair】自己是左边，自己把自己最后一对借给右边。这里应该调用CopyFirstFrom来给右边，但是不想调用了，就这么写这里先吧
  //MappingType pair = array_[GetSize()-1];
  MappingType pair = array_[GetSize()-1];
  
  //【找到父母页与相关】找到父母页面、recipient在父母页面array_上的位置
  auto *page = buffer_pool_manager->FetchPage(recipient->GetParentPageId()); //recipient和this的父母id应该是一样的
  auto *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(page->GetData());
  int index_in_parent = parent_page->ValueIndex(recipient->GetPageId()); //接受页在父母亲节点中的位置，要改这个位置上的key

  //【修改父母页】修改自己和recipient共同的父亲页面。找到parent中要修改的index，修改成pair.first
  parent_page->SetKeyAt(index_in_parent, pair.first);
  buffer_pool_manager->UnpinPage(parent_page->GetPageId(), true);

  //【修改接受页】接受页的接收
  //InsertNodeAfter会出问题
  for (int i = recipient->GetSize(); i > 0; --i) {
        recipient->array_[i] = recipient->array_[i - 1];
  }
  recipient->IncreaseSize(1); 
  recipient->array_[0] = pair;

  //【修改自己页】自己挪过去一个,size要减
  SetSize(GetSize()-1);

  //【修改被挪的孩子页】被挪了的孩子，父母节点id也要修改过去(这样才算挪过去其实)
  page_id_t child_page_id = pair.second;
  auto *another_page = buffer_pool_manager->FetchPage(child_page_id);
  auto *child_page = reinterpret_cast<BPlusTreePage *>(another_page->GetData());
  child_page->SetParentPageId(recipient->GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
//在前面加一个条目，和上面配合
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  //用InsertAfterNode是会使size增加1的。这里的假设是目标页原来的开头已经被删掉了？
  //这里怎么不给middle_key啊
  //其实这个函数不写也罢，集成在上面了已经
}

template
class BPlusTreeInternalPage<int, int, BasicComparator<int>>;

template
class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;

template
class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;

template
class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;

template
class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;

template
class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;