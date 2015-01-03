#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include <zmq.h>
#include <string.h>
#include <assert.h>

#include <signal.h>
#include <pthread.h>

#define MSGCANCOMMIT 1
#define MSGYES 2
#define MSGNO 3
#define MSGPRECOMMIT 4
#define MSGACK 5
#define MSGDOCOMMIT 6
#define MSGABORT 9

#define STATEFREE 10
#define STATEWAITING 11
#define STATEPREPARED 12


int m=0;
int pstate=STATEFREE;
int *msg;
const int numberOfClients=2;
int numberOfACK=0;
int rank, size, chanceOfNotSend=0;
void *context_Server = zmq_ctx_new ();
void *responder_Server = zmq_socket (context_Server, ZMQ_PULL);
int rc_Server = zmq_bind (responder_Server, "tcp://192.168.2.2:5555");

void* CCList[numberOfClients];
void* RCList[numberOfClients];

pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
pthread_t thread,thread_timer;

void aborted()
{
	pstate=STATEFREE;
	numberOfACK=0;
  return void();
};

void *timerfunction(void *i){
 	int a = *((int *) i);
  free(i);
	usleep(1000000);
	pthread_mutex_lock(&mutex1);
	if(pstate!=a){
		pthread_mutex_unlock(&mutex1);
		return NULL;
	}
  printf("TIMEOUT - ABORTED\n");
	for(int i=0;i<numberOfClients;i++)
	{
		int tmp=MSGABORT;
		zmq_send (RCList[i], &tmp, sizeof(tmp), ZMQ_NOBLOCK);
		printf("send %d\n",tmp);
	}
	aborted();
	pthread_mutex_unlock(&mutex1);
}

void broadcastWantCommit(){
	pthread_mutex_lock(&mutex1);
	if(pstate!=STATEFREE){
		pthread_mutex_unlock(&mutex1);
		return void();
	}
	for(int i=0;i<numberOfClients;i++)
		{

			int tmp=MSGCANCOMMIT;
			zmq_send (RCList[i], &tmp, sizeof(tmp), ZMQ_NOBLOCK);
			printf("send %d\n",tmp);

		}
		pstate=STATEWAITING;
		numberOfACK=0;
		int *arg =(int*)malloc(sizeof(*arg));
		*arg = pstate;
		pthread_create(&thread_timer, NULL, timerfunction, arg);
		pthread_mutex_unlock(&mutex1);
};
void broadcastPreCommit(){
	// printf("bpc1\n");
	pthread_mutex_lock(&mutex1);
	// printf("bpc2\n");
	if(pstate!=STATEWAITING){
		pthread_mutex_unlock(&mutex1);
		return void();
	}
	numberOfACK++;
	// printf("bpc3\n");
	if(numberOfACK<numberOfClients){
		pthread_mutex_unlock(&mutex1);
		return void();
	}
	// printf("bpc4\n");
	pthread_cancel(thread_timer);
	// printf("bpc5\n");
	for(int i=0;i<numberOfClients;i++)
	{

		int tmp=MSGPRECOMMIT;
		zmq_send (RCList[i], &tmp, sizeof(tmp), ZMQ_NOBLOCK);
		printf("send %d\n",tmp);
	}
	pstate=STATEPREPARED;
	numberOfACK=0;
	int *arg =(int*)malloc(sizeof(*arg));
	*arg = pstate;
	pthread_create(&thread_timer, NULL, timerfunction, arg);
	pthread_mutex_unlock(&mutex1);
};
void broadcastAbort(){
	pthread_mutex_lock(&mutex1);
	pthread_cancel(thread_timer);
	for(int i=0;i<numberOfClients;i++)
		{
			int tmp=MSGABORT;
			zmq_send (RCList[i], &tmp, sizeof(tmp), ZMQ_NOBLOCK);
			printf("send %d\n",tmp);
		}
		aborted();
		pthread_mutex_unlock(&mutex1);
};
void broadcastDoCommit(){
	pthread_mutex_lock(&mutex1);
	if(pstate!=STATEPREPARED){
		pthread_mutex_unlock(&mutex1);
		return void();
		}
	numberOfACK++;
	if(numberOfACK<numberOfClients){
			pthread_mutex_unlock(&mutex1);
			return void();
		}
	pthread_cancel(thread_timer);
	for(int i=0;i<numberOfClients;i++)
		{
			int tmp=MSGDOCOMMIT;
			zmq_send (RCList[i], &tmp, sizeof(tmp), ZMQ_NOBLOCK);
			printf("send %d\n",tmp);
		}
	pstate=STATEFREE;
	numberOfACK=0;
pthread_mutex_unlock(&mutex1);
	//	abort();
};

void controlUnit(int msg)
{
	//pstate=msg;

	switch(msg){
		case MSGCANCOMMIT: { printf("RECIV CANCOMMIT\n");broadcastWantCommit(); break;}
		case MSGYES: { printf("RECIV YES\n");broadcastPreCommit(); break;}
		case MSGNO: { printf("RECIV NO\n");broadcastAbort(); break;}
		case MSGACK: { printf("RECIV ACK\n");broadcastDoCommit(); break;}
	}
};
void init()
{
	for(int i=0;i<numberOfClients;i++)
		{
			CCList[i] = zmq_ctx_new ();
			RCList[i] = zmq_socket (CCList[i], ZMQ_PUSH);
		}
			zmq_connect (RCList[0], "tcp://192.168.2.1:5555");
			if(numberOfClients>1)
				zmq_connect (RCList[1], "tcp://192.168.2.3:5555");
}

int main (int argc, char* argv[])
{
	int wiad[1];
	int iter=3;
	int seed=time(NULL);
	srand(seed);
	init();
	printf( "Hello world from process " );
	wiad[0]=99;


	while(true){
		zmq_recv (responder_Server, wiad, sizeof(int), 0);
		int tmp=wiad[0];
		printf("dostalem wiadomosc %d\n",tmp);
		controlUnit(tmp);
//		free(msg);
	}
	for(int i=0;i<numberOfClients;i++)
		{
			zmq_close(RCList[i]);
			zmq_ctx_destroy(CCList[i]);
		}
	return 0;
}
