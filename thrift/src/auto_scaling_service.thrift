namespace cpp jiffy.auto_scaling

exception auto_scaling_exception {
  1: string msg
}

service auto_scaling_service {
  void auto_scaling(1: list<string> current_replica_chain, 2: string path, 3: map<string, string> conf)
    throws (1: auto_scaling_exception ex),
}