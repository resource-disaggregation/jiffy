namespace cpp jiffy.maestro

exception maestro_allocator_service_exception {
  1: string msg,
}

struct block {
  1: required string block_id,
  2: required i64 sequence_number,
}

service maestro_allocator_service {

  list<block> allocate(1: string tenantID, 2: i64 numBlocks )
    throws (1: maestro_allocator_service_exception ex),

  void deallocate(1: string tenentID, 2: list<block> blocks)
    throws (1: maestro_allocator_service_exception ex),
}

