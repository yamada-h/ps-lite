/*
  zmq -> ibverbs
*/

#ifndef PS_ZMQ_VAN_H_
#define PS_ZMQ_VAN_H_

//#define NUM_WORKERS 1
//#define NUM_SERVERS 1
#define MAX_SIZE 130000 //temporal value

#include <infiniband/verbs.h>
#include <zmq.h>
#include <stdlib.h>
#include <thread>
#include <string>
#include "ps/internal/van.h"

namespace ps {

    inline void FreeData(void *data, void *hint){
        if(hint == NULL){
            delete [] static_cast<char*>(data);
        }else{
            delete static_cast<SArray<char>*>(hint);
        }
    }

    class IBVVan : public Van {
    public:
        IBVVan() {}
        virtual ~IBVVan() {}



    private:

        typedef struct ibv_rdma_info{
            uint64_t addr;
            uint32_t rkey;
        } ibv_rdma_info_;

        typedef struct ibv_qp ibv_qp_;
        typedef struct ibv_mr ibv_mr_;
        typedef struct ibv_sge ibv_sge_;
        typedef struct ibv_recv_wr ibv_recv_wr_;
        typedef struct ibv_wc ibv_wc_;

        struct ibv_comp_channel * channel_;
        struct ibv_device **device_list_;

        void *receiver_ = nullptr;

        int port_lid = 0;
        int num_qp_;
        void *cq_context_;
        struct ibv_context *ibv_context_ = nullptr;
        void *zmq_context_ = nullptr;

        struct ibv_pd *pd_; /* only one protection domain */

        struct ibv_cq *send_cq_; 
        struct ibv_cq *recv_cq_; /* for heartbeat and msg */
        
        std::unordered_map<int, void*> senders_; /* for zmq connection sender */

        std::vector<ibv_mr_ *> send_mrs;
	    std::vector<ibv_mr_ *> recv_mrs;
        std::vector<ibv_qp_ *> qps;

        std::unordered_map<int, ibv_mr_*> send_mrs_;
        std::unordered_map<int, ibv_mr_*> recv_mrs_;
        std::unordered_map<int, ibv_qp_*> qps_; /* for ibverbs connection sender */

        std::unordered_map<int, ibv_rdma_info_> rdma_info_; /* remote addr & rkey */

        struct ibv_ah *ah_;
        bool ibv_flags = false; //preparation for ibv is complete or not
     
     protected:

         void Start() override {
            
            /* start ibverbs */
            int num_devices, i;

            Ib_addr tmp_ib;

            ibv_qp_ *tmp_qp;
            ibv_mr_ *tmp_send_mr;
	        ibv_mr_ *tmp_recv_mr;

            char *tmp_send_buffer;
            char *tmp_recv_buffer;

            /* SCHEDULER : NUM_SERVER + NUM_WORKER
               WORKER : NUM_SERVER + NUM_SCHEDULER(= 1)
               SERVER : NUM_WORKER + NUM_SCHEDULER(= 1) */
            if(is_scheduler_){
                num_qp_ = atoi(Environment::Get()->find("DMLC_NUM_WORKER")) + atoi(Environment::Get()->find("DMLC_NUM_SERVER"));
            }else{
                num_qp_ = Postoffice::Get()->is_server() ? atoi(Environment::Get()->find("DMLC_NUM_WORKER")) : atoi(Environment::Get()->find("DMLC_NUM_SERVER"));
                num_qp_ += 1;
            }

            device_list_ = ibv_get_device_list(&num_devices);
            struct ibv_device *device = device_list_[0];
            ibv_context_ = ibv_open_device(device);

            struct ibv_device_attr device_attr;
            ibv_query_device(ibv_context_, &device_attr);

            struct ibv_port_attr port_attr;
            ibv_query_port(ibv_context_, 1, &port_attr);

            pd_ = ibv_alloc_pd(ibv_context_);

            channel_ = ibv_create_comp_channel(ibv_context_);
            cq_context_ = NULL;

            /* create cq with completion channel */
            send_cq_ = ibv_create_cq(ibv_context_, 16, cq_context_, channel_, 0);
            recv_cq_ = ibv_create_cq(ibv_context_, 16, cq_context_, channel_ ,0);

            /* set zmq for IPoIB */
            zmq_context_ = zmq_ctx_new();
            //CHECK(zmq_context_ != NULL) << "create 0mq context failed";

            zmq_ctx_set(zmq_context_, ZMQ_MAX_SOCKETS, 65536);

            struct ibv_qp_init_attr attr;
            
            attr.send_cq = send_cq_;
            attr.recv_cq = recv_cq_;
            attr.qp_type = IBV_QPT_RC;
            attr.sq_sig_all = 1;

            struct ibv_qp_cap cap;
	        cap.max_send_wr =31;
	        cap.max_recv_wr = 31;

            attr.cap = cap;

            /* create QP and allocate MR */
            for(i = 0 ; i < num_qp_ ; i++){
                tmp_qp = ibv_create_qp(pd_, &attr);

		        tmp_send_buffer = (char *)malloc(MAX_SIZE);
		        tmp_recv_buffer = (char *)malloc(MAX_SIZE);

                tmp_send_mr = ibv_reg_mr(pd_, tmp_send_buffer, MAX_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);
		        tmp_recv_mr = ibv_reg_mr(pd_, tmp_recv_buffer, MAX_SIZE, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE);

                tmp_ib.qp_num = tmp_qp->qp_num;
                tmp_ib.port_num = 1;
                tmp_ib.lid = port_attr.lid;

                tmp_ib.addr = (uint64_t)tmp_recv_buffer;
                tmp_ib.rkey = tmp_recv_mr->rkey;

                /* for debug print */
		        PrintMyRole();
                std::cout << " port_num is :" << tmp_ib.port_num << std::endl;
                std::cout << " qp_num is :" << tmp_ib.qp_num << std::endl;
                std::cout << " lid is :" << tmp_ib.lid << std::endl;	
                std::cout << " addr is :" << tmp_ib.addr << std::endl;
                std::cout << " rkey is :" << tmp_ib.rkey << std::endl;
	
                qps.push_back(tmp_qp);

                recv_mrs.push_back(tmp_recv_mr);
		        send_mrs.push_back(tmp_send_mr);

                my_node_.ib_addr.push_back(tmp_ib);
            }
            Van::Start();
        }

        void Stop() override {
            PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
            Van::Stop();
            // close sockets
            int linger = 0;
            int rc = zmq_setsockopt(receiver_, ZMQ_LINGER, &linger, sizeof(linger));
            CHECK(rc == 0 || errno == ETERM);
            CHECK_EQ(zmq_close(receiver_), 0);
            for (auto& it : senders_) {
                int rc = zmq_setsockopt(it.second, ZMQ_LINGER, &linger, sizeof(linger));
                CHECK(rc == 0 || errno == ETERM);
                CHECK_EQ(zmq_close(it.second), 0);
            }
            zmq_ctx_destroy(zmq_context_);

            //close ibverbs
            for(int i = 0 ; i < num_qp_ ; i++){
                ibv_destroy_qp(qps[i]);
                ibv_dereg_mr(send_mrs[i]);
		        ibv_dereg_mr(recv_mrs[i]);
            }

            ibv_destroy_cq(recv_cq_);
            ibv_destroy_cq(send_cq_);
            ibv_dealloc_pd(pd_);
            ibv_close_device(ibv_context_);
            ibv_free_device_list(device_list_);

        }

        // for zmq function
        int Bind(const Node& node, int max_retry) override {
    		receiver_ = zmq_socket(zmq_context_, ZMQ_ROUTER);
    		//CHECK(receiver_ != NULL)
        	//<< "create receiver socket failed: " << zmq_strerror(errno);
    		int local = GetEnv("DMLC_LOCAL", 0);
    		std::string addr = local ? "ipc:///tmp/" : "tcp://*:";
    		int port = node.port;
    		unsigned seed = static_cast<unsigned>(time(NULL)+port);
    		for (int i = 0; i < max_retry+1; ++i) {
      			auto address = addr + std::to_string(port);
      			if (zmq_bind(receiver_, address.c_str()) == 0) break;
      			if (i == max_retry) {
        			port = -1;
      			} else {
        			port = 10000 + rand_r(&seed) % 40000;
      			}
    		}
    		return port;
  	    }

      void Connect_QPs(std::vector<Node>& node_list, Node& new_node){

            for(int i = 0 ; i < node_list.size() ; i++){  //node_offset ? or init value?
                auto& node = node_list[i];
                if(node.role != new_node.role){

                    node.ib_addr[node.offset].dest_qp_num = new_node.ib_addr[new_node.offset].qp_num;
                    node.ib_addr[node.offset].dest_addr = new_node.ib_addr[new_node.offset].addr;
                    node.ib_addr[node.offset].dest_lid = new_node.ib_addr[new_node.offset].lid;
                    node.ib_addr[node.offset].dest_psn = new_node.ib_addr[new_node.offset].psn;
                    node.ib_addr[node.offset].dest_rkey = new_node.ib_addr[new_node.offset].rkey;
        

                    new_node.ib_addr[new_node.offset].dest_qp_num = node.ib_addr[node.offset].qp_num;
                    new_node.ib_addr[new_node.offset].dest_addr = node.ib_addr[node.offset].addr;
                    new_node.ib_addr[new_node.offset].dest_lid = node.ib_addr[node.offset].lid;
                    new_node.ib_addr[new_node.offset].dest_psn = node.ib_addr[node.offset].psn;
                    new_node.ib_addr[new_node.offset].dest_rkey = node.ib_addr[node.offset].rkey;
		    
                    //std::cout << "dest_qp_num :" << node.ib_addr[node.offset].dest_qp_num << std::endl;
                    //std::cout << "dest_addr :" << node.ib_addr[node.offset].dest_addr << std::endl;
                    //std::cout << "dest_lid :" << node.ib_addr[node.offset].dest_lid << std::endl;
                    //std::cout << "dest_psn :" << node.ib_addr[node.offset].dest_psn << std::endl;
                    //std::cout << "dest_rkey :" << node.ib_addr[node.offset].dest_rkey << std::endl;
                    //std::cout << "new dest_rkey :" << new_node.ib_addr[node.offset].dest_rkey << std::endl;

                    node.ib_addr[node.offset].dest_id = new_node.id;
                    new_node.ib_addr[new_node.offset].dest_id = node.id;

                    node.offset++;
                    new_node.offset++;
                }
            }
      }

	void UpdateIDtoIBaddr(std::vector<Node>& node_list, Node& node){
		int i, j;
		int len;
		int length;
		uint64_t * dest_qp_numbers;
		for(auto& n : node_list){
			if(n.id == node.id){
				length = n.ib_addr.size();
				dest_qp_numbers = (uint64_t *)malloc(sizeof(uint64_t) * length);
				for(i = 0 ; i < length ; i++) dest_qp_numbers[i] = n.ib_addr[i].dest_qp_num;
			}
		}

		for(auto& nd : node_list){
			if(nd.id != node.id){
				len = nd.ib_addr.size();
				for(i = 0 ; i < len ; i++){
					for(j = 0 ; j < length ; j++){
						if(dest_qp_numbers[j] == nd.ib_addr[i].qp_num){
							nd.ib_addr[i].dest_id = node.id;
						}
					}
				}
			}
		}							

	}


    /* create zmq connection */
    /* only use for connect to Scheduler */
    void Connect_zmq(const Node& node){
        CHECK_NE(node.id, node.kEmpty);
        CHECK_NE(node.port, node.kEmpty);
        CHECK(node.hostname.size());

        int id = node.id;

        if((node.role == my_node_.role) && (node.id != my_node_.id)) return;

        /* set zmq for IPoIB */
        void *sender = zmq_socket(zmq_context_, ZMQ_REQ);
        if(my_node_.id != Node::kEmpty){
            std::string my_id = "ps" + std::to_string(my_node_.id);
            zmq_setsockopt(sender, ZMQ_IDENTITY, my_id.data(), my_id.size());
        }

        //Connect
        std::string addr = "tcp://" + node.hostname + ":" + std::to_string(node.port);
                
        if(GetEnv("DMLC_LOCAL", 0)){
            addr = "ipc:///tmp/" + std::to_string(node.port);
        }

        if(zmq_connect(sender, addr.c_str())) {
            LOG(FATAL) << "connect to " + addr + "failed: " + zmq_strerror(errno);
        }

        senders_[id] = sender;
    }


        
    /* create CQ & QP & MR, and exchange QP info each other, and throw initial WR into RQ */
    /* node == my_node_ */
    void Connect(const Node& node) override {
        
        CHECK_NE(node.id, node.kEmpty);
        CHECK_NE(node.port, node.kEmpty);
        CHECK(node.hostname.size());

        //int id = node.id;
        int index_id;

	    my_node_.id = node.id;

        for(int i = 0 ; i < qps.size() ; i++){
                
            if( qps[i]->qp_num != node.ib_addr[i].qp_num ){
                std::cout << "Error: invalid association \n"; 
             }

                /* Reset -> Init */
                struct ibv_qp_attr init_attr;
                init_attr.qp_state = IBV_QPS_INIT;
                init_attr.pkey_index = 0;
                init_attr.port_num = 1; //only one-port ( multi-port is not assumed )
                init_attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
                init_attr.qkey = 0x00000001;
            

                int ret = ibv_modify_qp(qps[i], &init_attr, IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
                if(ret != 0){
                    std::cout << "Error: " << ret << "," << strerror(ret) << "\n";
                    return;
                }

                /* Init -> RtR */
                struct ibv_ah_attr ah_attr;
                ah_attr.dlid = node.ib_addr[i].dest_lid;
                ah_attr.port_num = 1;
                ah_ = ibv_create_ah(pd_, &ah_attr);

                struct ibv_qp_attr rtr_attr;
                rtr_attr.qp_state = IBV_QPS_RTR;
                rtr_attr.path_mtu = IBV_MTU_4096;
                rtr_attr.ah_attr = ah_attr;
                rtr_attr.dest_qp_num = node.ib_addr[i].dest_qp_num;
                rtr_attr.rq_psn = node.ib_addr[i].dest_psn;
                rtr_attr.max_dest_rd_atomic = 16;
                rtr_attr.min_rnr_timer = 0; // infinite
                
                ret = ibv_modify_qp(qps[i], &rtr_attr, IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_AV | IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER);
                if(ret != 0){
                    std::cout << "Error: " << ret << "," << strerror(ret) << "\n";
                    return;
                }

                /* RtR -> RtS */
                struct ibv_qp_attr rts_attr;
                rts_attr.qp_state = IBV_QPS_RTS;
                rts_attr.sq_psn = 0;
                rts_attr.timeout = 0;
                rts_attr.retry_cnt = 7;
                rts_attr.rnr_retry = 7;
                rts_attr.max_rd_atomic = 0;
                
                ret = ibv_modify_qp(qps[i], &rts_attr, IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC); //RC
                if(ret != 0){
                    std::cout << "Error: " << ret << "," << strerror(ret) << "\n";
                    return;
                }
            
                /* vector -> unordered map */
                index_id = node.ib_addr[i].dest_id;
                rdma_info_[index_id].addr = node.ib_addr[i].dest_addr;
                rdma_info_[index_id].rkey = node.ib_addr[i].dest_rkey;
                
                qps_[index_id] = qps[i];
                recv_mrs_[index_id] = recv_mrs[i];
		        send_mrs_[index_id] = send_mrs[i];

                /* using only one sge */
                ibv_sge_ sge;
                sge.addr = (uint64_t)recv_mrs[i]->addr;
                sge.length = MAX_SIZE;
                sge.lkey = recv_mrs[i]->lkey;

                ibv_recv_wr_ recv_wr = {
                    .wr_id = index_id,
                    .next = NULL,
                    .sg_list = &sge,
                    .num_sge = 1,
                };
                
                /* for debug print */
		        std::cout << "@ id " << my_node_.id << " ---- " << index_id << ", connect QP number : " << qps_[index_id]->qp_num << " --->> " << node.ib_addr[i].dest_qp_num << std::endl;

                /* register initial Work Request to Recieve Queue */
                ibv_recv_wr_ *bad_wr;
                ret = ibv_post_recv(qps[i], &recv_wr, &bad_wr);
            
            }
            /* complete preparation for RDMA Operation */
            int ret_val = ibv_req_notify_cq(recv_cq_, 0);
            ibv_flags = true;

        }


        int SendZmq(const Message& msg) {
            std::lock_guard<std::mutex> lk(mu_);

            // find the socket
            int id = msg.meta.recver;
            CHECK_NE(id, Meta::kEmpty);

            auto it = senders_.find(id);
            if (it == senders_.end()) {
                LOG(WARNING) << "there is no socket to node " << id;
                return -1;
            }
            void *socket = it->second;

            // send meta
            int meta_size; char* meta_buf;
            
            PackMeta(msg.meta, &meta_buf, &meta_size);
            
            int tag = ZMQ_SNDMORE;
            int n = msg.data.size();
            if (n == 0) tag = 0;

            zmq_msg_t meta_msg;
            zmq_msg_init_data(&meta_msg, meta_buf, meta_size, FreeData, NULL);
            
            while (true) {
                if (zmq_msg_send(&meta_msg, socket, tag) == meta_size) break;
                if (errno == EINTR) continue;
                LOG(WARNING) << "failed to send message to node [" << id
                        << "] errno: " << errno << " " << zmq_strerror(errno);
                return -1;
            }

            zmq_msg_close(&meta_msg);
            int send_bytes = meta_size;
            
            // send data
            for (int i = 0; i < n; ++i) {
                zmq_msg_t data_msg;

                SArray<char>* data = new SArray<char>(msg.data[i]);
                int data_size = data->size();

                /* zero-copy */
                zmq_msg_init_data(&data_msg, data->data(), data->size(), FreeData, data);
                if (i == n - 1) tag = 0;
                
                while (true) {
                    if (zmq_msg_send(&data_msg, socket, tag) == data_size) break;
                    if (errno == EINTR) continue;
                    LOG(WARNING) << "failed to send message to node [" << id
                                << "] errno: " << errno << " " << zmq_strerror(errno)
                                << ". " << i << "/" << n;
                    return -1;
                }
                zmq_msg_close(&data_msg);
                send_bytes += data_size;
            }
            return send_bytes;
        }


        /* specify QP & MR from msg's meta, and serialize msg, and throw WR into SQ */
        /* To Do: implement resender mechanism */
        int SendIBV(const Message& msg){

            std::lock_guard<std::mutex> lk(mu_);

            int id = msg.meta.recver;
            int count = 0;
            int i;
            CHECK_NE(id, Meta::kEmpty);
            
            /*
	        if(msg.meta.control.cmd == Control::ADD_NODE){
		        std::cout << "It's ADD_NODE command @ " << my_node_.id << std::endl;
            }else if(msg.meta.control.cmd == Control::BARRIER){
		        std::cout << "It's Barrier command @ " << my_node_.id << std::endl;
	        }else{
		        std::cout << "It's another command @ " << my_node_.id << std::endl;
	        }
	        */

            /* find QP, MR, Remote addr for recver node */
            auto it_qp = qps_.find(id);
            if (it_qp == qps_.end()){
                LOG(WARNING) << "There is no QP to recver: " << id << " @ " << my_node_.id;
                return -1;
            }
            struct ibv_qp *qp_ = it_qp->second;

            auto it_mr = send_mrs_.find(id);
            if (it_mr == send_mrs_.end()){
                LOG(WARNING) << "There is no MR to recver" << id;
                return -1;
            }
            ibv_mr_ *mr = it_mr->second;

            auto it_addr = rdma_info_.find(id);
            if (it_addr == rdma_info_.end()){
                LOG(WARNING) << "There is no Remote Address to recver" << id;
                return -1;
            }
            ibv_rdma_info_ rdma_info_ = it_addr->second;

            // create meta
            int meta_size, msg_data_size;
            char *buf_tmp = (char *)mr->addr;
            int n = msg.data.size();
            SArray<char>* data;

            PackMeta_IBV(msg.meta, &buf_tmp, &meta_size);

            //std::cout << "msg from " << qp_->qp_num << ", meta_size is ::::: " << meta_size << " @ " << my_node_.id << " --> " << id << std::endl;

            size_t send_bytes = meta_size;

            for (i = 0 ; i < n ; i++){
                
                data = new SArray<char>(msg.data[i]);
                int data_size = data->size();

                /* selialize msg.data */
                memcpy((mr->addr + send_bytes), data, data_size);
                send_bytes += data_size;   
                
            }

            ibv_sge_ sges; // Meta & Data

            /* Meta + Data entry */
            sges.addr = (uint64_t)mr->addr;
            sges.length = send_bytes;
            sges.lkey = mr->lkey;
            
            struct ibv_send_wr send_wr_;
            send_wr_.wr_id = id; //64-bit aruzo !!
            send_wr_.next = NULL;
            send_wr_.sg_list = &sges;
            send_wr_.num_sge = 1;
            send_wr_.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
            send_wr_.imm_data = meta_size;
            send_wr_.wr.rdma.remote_addr = rdma_info_.addr;
            send_wr_.wr.rdma.rkey = rdma_info_.rkey;
            //std::cout << "mr->addr : " << mr->addr << ", length : " << send_bytes << std::endl;
           // std::cout << "id : " << id << ", remote_addr : " << send_wr_.wr.rdma.remote_addr << ", rkey : " << send_wr_.wr.rdma.rkey << std::endl;


            struct ibv_send_wr *bad_wr_;
            ibv_wc_ wc[1];
            
            /* for debugging print */
            if(n > 0) std::cout << "before ibv_post_send with data @ " << my_node_.id << std::endl;

            int ret = ibv_post_send(qp_, &send_wr_, &bad_wr_);
	    
            if(ret != 0){
                LOG(WARNING) << "Failed to ibv_post_send : " << ret << " @ " << my_node_.id << " --> " << id; 
                return -1;
            }else{
		        std::cout << "Success to ibv_post_send @ " << my_node_.id << ", send_bytes : " << send_bytes << std::endl;
            }
 	    

            /*
            ibv_req_notify_cq(send_cq_, 0);
            ret = ibv_get_cq_event(channel_, &send_cq_, &cq_context_);
            ibv_ack_cq_events(send_cq_, 1);
            */

            do{
                ret = ibv_poll_cq(send_cq_, num_qp_, wc);
                count++;
            }while(ret == 0);
            
            if(wc[0].status != IBV_WC_SUCCESS){
		        std::cout << "Failed to Operation: status: " << wc[0].status <<  ", from " << my_node_.id << " to " << id << std::endl;
            }else{
		        std::cout << "IBV_POST_SEND success!!  send " << send_bytes << " bytes, from " << my_node_.id << " --> " << id <<  std::endl;
            }
            return send_bytes;
        }


        /* Receive function for zmq */
        int RecvZmq(Message* msg){
            msg->data.clear();
            size_t recv_bytes = 0;
            for (int i = 0; ; ++i) {
                zmq_msg_t* zmsg = new zmq_msg_t;
                CHECK(zmq_msg_init(zmsg) == 0) << zmq_strerror(errno);
                while (true) {
                    if (zmq_msg_recv(zmsg, receiver_, 0) != -1) break;
                    if (errno == EINTR) continue;
                    LOG(WARNING) << "failed to receive message. errno: "
                                << errno << " " << zmq_strerror(errno);
                    return -1;
                }
                char* buf = CHECK_NOTNULL((char *)zmq_msg_data(zmsg));
                size_t size = zmq_msg_size(zmsg);

                recv_bytes += size;

                if (i == 0) {
                    // identify
                    msg->meta.sender = GetNodeID(buf, size);
                    msg->meta.recver = my_node_.id;
                    CHECK(zmq_msg_more(zmsg));
                    zmq_msg_close(zmsg);
                    delete zmsg;
                } else if (i == 2) { // unresolved
                    // task
                    UnpackMeta(buf, size, &(msg->meta));
                    zmq_msg_close(zmsg);
                    bool more = zmq_msg_more(zmsg);
                    delete zmsg;
                    if (!more) break;
                } else if(i > 2) {
                    // zero-copy
                    SArray<char> data;
                    data.reset(buf, size, [zmsg, size](char* buf) {
                    zmq_msg_close(zmsg);
                        delete zmsg;
                    });
                    msg->data.push_back(data);
                    if (!zmq_msg_more(zmsg)) { break; }
                }
            }
            return recv_bytes;
        }

        /* Receive function for ibv */
        int RecvIBV(Message* msg){

            int ret, meta_size, total_size, process_size;
            int num_wc;
            ibv_wc_ wc[num_qp_];
            ibv_recv_wr_ *bad_wr;
            ibv_mr_ *mr;
            SArray<char> data;

            struct ibv_qp_attr *attr;
    	    ibv_qp_attr_mask attr_mask;
	        struct ibv_qp_init_attr* init_attr;

            
	        std::cout << "I will get completion event @ " << my_node_.id <<  std::endl;
	        // blocking function 
            //ret = ibv_get_cq_event(channel_, &recv_cq_, &cq_context_);

            //ibv_ack_cq_events(recv_cq_, 1);

	        //ret = ibv_req_notify_cq(recv_cq_, 0);

            //std::cout << "completion channel works!!!" << std::endl;
            /* get Work Completion */

            do { 
                num_wc = ibv_poll_cq(recv_cq_, 1, wc); // temporary  set second argument : 1
            } while (num_wc == 0);
            //std::cout << "get completion......." << std::endl;
            if(wc[0].status != IBV_WC_SUCCESS){
                LOG(WARNING) << "Completion Error " << wc[0].status << ", @ " << my_node_.id << " for " << wc[0].wr_id;

		int hoge = ibv_query_qp(qps_[wc[0].wr_id], attr, attr_mask, init_attr);
                std::cout << "cur_qp_state is : " << attr->cur_qp_state << std::endl;
                return -1;
            }

            /* for all work completion */
            for(int i = 0 ; i < num_wc ; i++){



                /* wc.wr_id has senders id */
                /* get qp & mr */
                auto it = qps_.find(wc[i].wr_id);
                ibv_qp_ *qp = it->second;

                auto it_mr = recv_mrs_.find(wc[i].wr_id);
                mr = it_mr->second;

		        std::cout << "receiving @ " << my_node_.id << " <-- " << wc[i].wr_id << ", meta_size is " << wc[i].imm_data << std::endl;

                /* identify */
                meta_size = wc[i].imm_data;
                msg->meta.sender = wc[i].wr_id;
                msg->meta.recver = my_node_.id;

                UnpackMeta((char *)mr->addr, meta_size, &(msg->meta));

                /*
                if(msg->meta.control.cmd == Control::ADD_NODE){
			        std::cout << "Receiving ADD_NODE @ " << my_node_.id << std::endl;
		        }else if(msg->meta.control.cmd == Control::BARRIER){
			        std::cout << "Receiving BARRIER @ " << my_node_.id << std::endl;
		        }else{
			        std::cout << "Receiving another command @ " << my_node_.id << std::endl;
		        }
		        */

                total_size = wc[i].byte_len;
                total_size -= meta_size;

                process_size = meta_size;

                //std::cout << "msg->meta.data-num : " << msg->meta.data_num << std::endl;   
		        for(int j = 0 ; j < msg->meta.data_num ; ++j){

                    std::cout << "msg->meta.data_size --- " << msg->meta.data_size[j] << " reset_mr address : " << mr->addr + process_size << " @ " << my_node_.id << std::endl;
			        uint64_t *sample = (uint64_t *)(mr->addr + process_size);

                    /* I want to delete this part.... */
                    char* tmp_reset_mr = (char *)malloc(msg->meta.data_size[j]);
                    memcpy(tmp_reset_mr, mr->addr + process_size, msg->meta.data_size[j]);
                	
                    data.reset_mr(tmp_reset_mr, msg->meta.data_size[j]);
                	msg->data.push_back(data);
                    process_size += msg->meta.data_size[j];
		        }
 
                /* next work request */
                ibv_sge_ sge = {
                    .addr = (uint64_t)mr->addr,
                    .length = MAX_SIZE,
                    .lkey = mr->lkey,
                };

                ibv_recv_wr_ recv_wr = {
                    .wr_id = wc[i].wr_id,
                    .next = NULL,
                    .sg_list = &sge,
                    .num_sge = 1,
                };
 
		        ibv_sge_ sge1 = {
                    .addr = (uint64_t)mr->addr,
                    .length = MAX_SIZE,
                    .lkey = mr->lkey,
                };

                ibv_recv_wr_ recv_wr1 = {
                    .wr_id = wc[i].wr_id,
                    .next = NULL,
                    .sg_list = &sge1,
                    .num_sge = 1,
                };

                ret = ibv_post_recv(qp, &recv_wr, &bad_wr);

		        //ret = ibv_post_recv(qp, &recv_wr1, &bad_wr);

            std::cout << "Finish IBV_POST_RECV!! from " << my_node_.id << " <---- " << wc[i].wr_id <<  std::endl;
            }

	    return process_size;

        }

        /* 1.connect QP between node in node_list and new_node
           2.exchange QP_NUM & ADDR(RDMA with IMM)
           3.Only called by Scheduler */
        
        
        /* getting completion, and specify QP from wc.wr_id, and throw next WR into it */
        /* To Do : process of recieved msg (e.g. seperate meta - data)
                   consider about next req_notify timing */
        int RecvMsg(Message *msg) override {
            int ret;
            msg->data.clear();
            if(!ibv_flags){
                ret = RecvZmq(msg);
            }else{
                ret = RecvIBV(msg);
            }
	    return ret;
        }
             
        int SendMsg(const Message& msg) override{
            int ret;
            if(!ibv_flags){
                ret = SendZmq(msg);
            }else{
                ret = SendIBV(msg);
            }
            return ret;
        }
                

     int GetNodeID(const char* buf, size_t size) {
    	if (size > 2 && buf[0] == 'p' && buf[1] == 's') {
      	int id = 0;
      	size_t i = 2;
      	for (; i < size; ++i) {
        	if (buf[i] >= '0' && buf[i] <= '9') {
          		id = id * 10 + buf[i] - '0';
        	} else {
          		break;
        	}
        }
      	if (i == size) return id;
    	}
    	return Meta::kEmpty;
	}

     void PrintMyRole(){
	if(Postoffice::Get()->is_scheduler()){
		std::cout << "----scheduler----" << std::endl;
        }else if(Postoffice::Get()->is_worker()){
		std::cout << "----worker----" << std::endl;
	}else{
		std::cout << "----server----" << std::endl;
	}
     }

    std::mutex mu_;

    };
}
#endif
