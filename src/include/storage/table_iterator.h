#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"


class TableHeap;

class TableIterator {

public:
  // you may define your own constructor based on your member variables
  TableIterator();
  
  TableIterator(TableHeap *_table_heap, RowId _rid);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;
  Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

  TableIterator& operator=(const TableIterator &other);

private:
  // add your own private member variables here
  TableHeap *table_heap;//use a point, query a table heap
  Row *row;//this iterator is used to query the turple of the table, this is used to record the turple
  Transaction* txn_;
  //page_id_t current_page_id;
  //u_int32_t current_slot_id;
  // add your own private member variables here
};

#endif //MINISQL_TABLE_ITERATOR_H