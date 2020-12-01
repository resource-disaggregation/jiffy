#include "bheap.h"

// class BroadcastHeap:
// 	def __init__(self):
// 		self.base_val = 0
// 		self.h = []

// 	def push(self, key, val):
// 		heappush(self.h, (val-self.base_val, key))
	
// 	def pop(self):
// 		val, key = heappop(self.h)
// 		return (key, val + self.base_val)

// 	def min_val(self):
// 		return self.h[0][0] + self.base_val

// 	def size(self):
// 		return len(self.h)

// 	def add_to_all(self, delta):
// 		self.base_val += delta


namespace jiffy {
namespace directory {



BroadcastHeap::BroadcastHeap() {
    base_val_ = 0;
}

void BroadcastHeap::push(const std::string &key, int32_t value) {
    h_.push(std::make_pair(key, value - base_val_));
}

bheap_item BroadcastHeap::pop() {
    bheap_item x = h_.top();
    h_.pop();
    return std::make_pair(x.first, x.second + base_val_);
}

int32_t BroadcastHeap::min_val() {
    return h_.top().second + base_val_;
}

void BroadcastHeap::add_to_all(int32_t delta) {
    base_val_ += delta;
}

std::size_t BroadcastHeap::size() {
    return h_.size();
}

}
}