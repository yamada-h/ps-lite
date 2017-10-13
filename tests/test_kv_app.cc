#include "ps/ps.h"
#include "unistd.h"
using namespace ps;

void StartServer() {
  if (!IsServer()) return;
  auto server = new KVServer<float>(0);
  server->set_request_handle(KVServerDefaultHandle<float>());
  RegisterExitCallback([server](){ delete server; });
}

void RunWorker() {
  if (!IsWorker()) return;
  KVWorker<float> kv(0);

  // init
  int num = 1000;
  std::vector<Key> keys(num);
  std::vector<float> vals(num);

  int rank = MyRank();
  srand(rank + 7);
  for (int i = 0; i < num; ++i) {
    keys[i] = kMaxKey / num * i + rank;
    vals[i] = (rand() % 1000);
  }

  // push
  int repeat = 50;
  std::vector<int> ts;
  for (int i = 0; i < repeat; ++i) {
    ts.push_back(kv.Push(keys, vals));
    std::cout << "ts[i] : " << ts[i] << std::endl;
    // to avoid too frequency push, which leads huge memory usage
    if (i > 10) kv.Wait(ts[ts.size()-10]);
  }
  std::cout << "before kv.wait....." << std::endl;
  for (int t : ts) kv.Wait(t);
  std::cout << "end kv.watit ......" << std::endl;

  // pull
  std::vector<float> rets;
  std::cout << "before pull requet" << std::endl;
  kv.Wait(kv.Pull(keys, &rets));

  float res = 0;
  std::cout << "after pull request " << std::endl;
  for (int i = 0; i < num; ++i) {
    res += fabs(rets[i] - vals[i] * repeat);
  }
  CHECK_LT(res / repeat, 1e-5);
  LL << "error: " << res / repeat;
}

int main(int argc, char *argv[]) {
  // setup server nodes
  /*
  if(IsWorker()){
  	std::cout << "before start worker\n";
  }else if(IsServer()){
	std::cout << "before start server\n";
  }else{
	std::cout << "before start scheduler\n";
  }
  */
  StartServer();
  /*
  if(IsWorker()){
        std::cout << "after start worker\n";
  }else if(IsServer()){
        std::cout << "after start server\n";
  }else{
        std::cout << "after start scheduler\n";
  }
  */

  // start system
  Start();
  /*
  if(IsWorker()){
        std::cout << "before run worker\n";
  }else if(IsServer()){
        std::cout << "before run server\n";
  }else{
        std::cout << "before run scheduler\n";
  }
  */

  // run worker nodes
  RunWorker();
  // stop system
  Finalize();
  return 0;
}
