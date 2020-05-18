namespace cpp jiffy.maestro

exception maestro_allocator_service_exception {
  1: string msg,
}

service maestro_allocator_service {

  list<string> allocate(1: string tenantID, 2: i64 numBlocks )
    throws (1: maestro_allocator_service_exception ex),

  void deallocate(1: string tenentID, 2: list<string> blocks)
    throws (1: maestro_allocator_service_exception ex),
}

