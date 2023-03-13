#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  // 需要自己定义成员变量我擦
  explicit IndexIterator(int index_in_page, B_PLUS_TREE_LEAF_PAGE_TYPE *current_page, BufferPoolManager *buffer_pool_manager);

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  const MappingType &operator*();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
  
  int index_in_page_; //当前在页中的偏移位置

  B_PLUS_TREE_LEAF_PAGE_TYPE *current_page_; //当前所在的页

  BufferPoolManager *buffer_pool_manager_;
};


#endif //MINISQL_INDEX_ITERATOR_H
