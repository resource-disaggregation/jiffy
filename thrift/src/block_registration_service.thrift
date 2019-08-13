namespace cpp jiffy.directory

exception block_registration_service_exception {
  1: string msg,
}

service block_registration_service {
  void add_blocks(1: list<string> block_ids)
    throws (1: block_registration_service_exception ex),

  void remove_blocks(1: list<string> block_ids)
    throws (1: block_registration_service_exception ex),
}
