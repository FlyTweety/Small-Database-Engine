#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages); 
  //这个num_pages是构造时输入的，要记下。后文要自己定义一个lru_list，这个当作其容量。或者只用于上界检测，超过时就开始删。lru_list就用vector
  //上面引用了vector和list 好像在暗示我
  //需要自己定义一个lru_list，每个元素应当是对应的逻辑页号
  //主要问题：怎么知道哪些是最近最少访问的？怎么知道有没有被访问�?
  //每次vector里第一个就是要替换出去�?

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here
  //这里需要定义的除了lru_list还有吗？
  //还有一个数组记录有没有被pin
  size_t max_pages; //记录num_pages
  vector<frame_id_t> lru_list; //网站上叫做lru_list_，我不理�?
};

#endif  // MINISQL_LRU_REPLACER_H
