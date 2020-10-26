/* server.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/

#include <signal.h>
#include <sys/stat.h>
#include "utils.h"
#include "kissdb.h"
#include <sys/time.h>
#include <stdio.h>
#include <pthread.h>
#include <semaphore.h>

#define MY_PORT                 6767
#define BUF_SIZE                1160
#define KEY_SIZE                 128
#define HASH_SIZE               1024
#define VALUE_SIZE              1024
#define MAX_PENDING_CONNECTIONS   10
#define QUEUE_SIZE 1000


#define THREADS 4
//------------Initialization of threads, sems. and mutex-------//
pthread_t thread_producer;
pthread_t thread_consumers[THREADS];
pthread_mutex_t queue_mutex;
pthread_mutex_t db_mutex;
pthread_cond_t db_cond_variable;
pthread_cond_t queue_cond_variable;


//---------- Initialization of the node structure--------//
typedef struct queue_node{
  int connection_socket_fd;
  double connection_start_time;
  double removed_from_queue_time;
  double start_of_processing_the_request_time;
  double end_of_processing_the_request_time;
}Queue_node;


Queue_node *common_data_queue[QUEUE_SIZE]; /*initialize of the common data array*/
int head,tail = 0;
int writer_count, reader_count = 0;
double total_waiting_time = 0;
double total_service_time = 0;
int completed_requests = 0;
int checking_for_threads_termination = 0;
int total_entrances = 0;


void process_request(const int socket_fd);

// Definition of the operation type.
typedef enum operation {
  PUT,
  GET
} Operation; 

// Definition of the request.
typedef struct request {
  Operation operation;
  char key[KEY_SIZE];  
  char value[VALUE_SIZE];
} Request;

typedef struct pthread_create_arguments{
  char request_str[BUF_SIZE];
  int  sock_fd;
}Pthread_create_arguments;

// Definition of the database.
KISSDB *db = NULL;

/**
 * @name parse_request - Parses a received message and generates a new request.
 * @param buffer: A pointer to the received message.
 *
 * @return Initialized request on Success. NULL on Error.
 */
Request *parse_request(char *buffer) {
  char *token = NULL;
  Request *req = NULL;
  
  // Check arguments.
  if (!buffer)
    return NULL;
  
  // Prepare the request.
  req = (Request *) malloc(sizeof(Request));
  memset(req->key, 0, KEY_SIZE);
  memset(req->value, 0, VALUE_SIZE);

  // Extract the operation type.
  token = strtok(buffer, ":");    
  if (!strcmp(token, "PUT")) {
    req->operation = PUT;
  } else if (!strcmp(token, "GET")) {
    req->operation = GET;
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the key.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->key, token, KEY_SIZE);
  } else {
    free(req);
    return NULL;
  }
  
  // Extract the value.
  token = strtok(NULL, ":");
  if (token) {
    strncpy(req->value, token, VALUE_SIZE);
  } else if (req->operation == PUT) {
    free(req);
    return NULL;
  }
  return req;
}

void *incoming_request_handler_by_the_consumers(void *arg){
  struct timeval tv;
	while (checking_for_threads_termination != 1) {
    Queue_node *pointer_to_the_queue_node = NULL;

		pthread_mutex_lock(&queue_mutex);
		while(total_entrances == 0){
      pthread_cond_wait(&queue_cond_variable, &queue_mutex);
      if(checking_for_threads_termination == 1){
        pthread_mutex_unlock(&queue_mutex);
        return EXIT_SUCCESS;
      }
    }
    gettimeofday(&tv, NULL);
    if(head == QUEUE_SIZE){
      head = 0;
    }
		pointer_to_the_queue_node = common_data_queue[head];
    head += 1;
    total_entrances -=1;

		pointer_to_the_queue_node->removed_from_queue_time = (tv.tv_sec + tv.tv_usec/1000000.0);
    
    pthread_cond_broadcast(&queue_cond_variable);
		pthread_mutex_unlock(&queue_mutex);
    
    gettimeofday(&tv, NULL);
		pointer_to_the_queue_node->start_of_processing_the_request_time = (tv.tv_sec + tv.tv_usec/1000000.0);
    process_request(pointer_to_the_queue_node->connection_socket_fd);
    
  
		gettimeofday(&tv, NULL);
		pointer_to_the_queue_node->end_of_processing_the_request_time = (tv.tv_sec + tv.tv_usec/1000000.0);
		
		pthread_mutex_lock(&queue_mutex);
    total_waiting_time += pointer_to_the_queue_node->removed_from_queue_time - pointer_to_the_queue_node->connection_start_time;   
    total_service_time += pointer_to_the_queue_node->end_of_processing_the_request_time - pointer_to_the_queue_node->start_of_processing_the_request_time;
		completed_requests += 1;
		pthread_mutex_unlock(&queue_mutex);
		
		close(pointer_to_the_queue_node->connection_socket_fd);
		free(pointer_to_the_queue_node);
	}
	
	return EXIT_SUCCESS;			
}


/*
 * @name process_request - Process a client request.
 * @param socket_fd: The accept descriptor.
 *
 * @return
 */
void process_request(const int socket_fd) {

  char response_str[BUF_SIZE], request_str[BUF_SIZE];
    int numbytes = 0;
    Request *request = NULL;

    // Clean buffers.
    memset(response_str, 0, BUF_SIZE);
    memset(request_str, 0, BUF_SIZE);

    // receive message.
    numbytes = read_str_from_socket(socket_fd, request_str, BUF_SIZE);

    // parse the request.
    if (numbytes) {
      request = parse_request(request_str);
      if (request) {
        switch (request->operation) {
          case GET:
            pthread_mutex_lock(&db_mutex);
            while(writer_count == 1){
              pthread_cond_wait(&db_cond_variable, &db_mutex);
            }
            reader_count += 1;
            pthread_mutex_unlock(&db_mutex);
            // Read the given key from the database.
            if (KISSDB_get(db, request->key, request->value))
              sprintf(response_str, "GET ERROR\n");
            else
              sprintf(response_str, "GET OK: %s\n", request->value);

            if(writer_count > 0 && reader_count > 0){
              printf("ERROR,in put_get sychronization!!\n");
            }

            pthread_mutex_lock(&db_mutex);
            reader_count -= 1;
            pthread_cond_broadcast(&db_cond_variable);
            pthread_mutex_unlock(&db_mutex);
            break;
          case PUT:
            pthread_mutex_lock(&db_mutex);
            while(writer_count == 1 || reader_count >= 1){
              pthread_cond_wait(&db_cond_variable, &db_mutex);
            }
            writer_count += 1;
            // Write the given key/value pair to the database.
            if (KISSDB_put(db, request->key, request->value)) 
              sprintf(response_str, "PUT ERROR\n");
            else
              sprintf(response_str, "PUT OK\n");

            if(writer_count > 0 && reader_count > 0){
              printf("ERROR,in put_get sychronization!!\n");
            }

            writer_count -= 1;
            pthread_cond_broadcast(&db_cond_variable);
            pthread_mutex_unlock(&db_mutex);
            break;

          default:
            // Unsupported operation.
            sprintf(response_str, "UNKOWN OPERATION\n");
        }
        // Reply to the client.
        write_str_to_socket(socket_fd, response_str, strlen(response_str));
        if (request)
          free(request);
        request = NULL;
        return;
      }
    }
    // Send an Error reply to the client.
    sprintf(response_str, "FORMAT ERROR\n");
    write_str_to_socket(socket_fd, response_str, strlen(response_str));
}

static void handle_SIGTSTP(int sig_no){
  checking_for_threads_termination = 1;
  pthread_cond_broadcast(&queue_cond_variable);
	
	int i;
	for(i = 0; i<THREADS; i++){
    pthread_join(thread_consumers[i], NULL);
  }
	
  pthread_mutex_destroy(&queue_mutex);
  pthread_mutex_destroy(&db_mutex);
  pthread_cond_destroy(&db_cond_variable);
  pthread_cond_destroy(&queue_cond_variable);
    	
	// Destroy the database.
	// Close the database.
	KISSDB_close(db);
  
    // Free memory.
    if (db)
    free(db);
    db = NULL;
	
	printf("\nXronoi se sec:\n");
	printf("\ttotal_waiting_time: %f\n", total_waiting_time/THREADS);
	printf("\ttotal_service_time: %f\n", total_service_time/THREADS);
  printf("completed_requests: %d\n", completed_requests);
  
  exit(EXIT_SUCCESS);
}

/*
 * @name main - The main routine.
 *
 * @return 0 on success, 1 on error.
 */
int main() {
  //----------------My code--------------////
  int socket_fd;              // listen on this socket for new connections
  //    ,new_fd;                 // use this socket to service a new connection
  socklen_t clen;
  struct sockaddr_in server_addr,  // my address information
                     client_addr;  // connector's address information

  // create socket
  if ((socket_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
    ERROR("socket()");

  // Ignore the SIGPIPE signal in order to not crash when a
  // client closes the connection unexpectedly.
  signal(SIGPIPE, SIG_IGN);
  
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_ANY);    // any local interface
  server_addr.sin_port = htons(MY_PORT);
  
  // bind socket to address
  if (bind(socket_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) == -1)
    ERROR("bind()");
  
  // start listening to socket for incomming connections
  listen(socket_fd, MAX_PENDING_CONNECTIONS);
  fprintf(stderr, "(Info) main: Listening for new connections on port %d ...\n", MY_PORT);
  clen = sizeof(client_addr);

  // Allocate memory for the database.
  if (!(db = (KISSDB *)malloc(sizeof(KISSDB)))) {
    fprintf(stderr, "(Error) main: Cannot allocate memory for the database.\n");
    return 1;
  }
  
  // Open the database.
  if (KISSDB_open(db, "mydb.db", KISSDB_OPEN_MODE_RWCREAT, HASH_SIZE, KEY_SIZE, VALUE_SIZE)) {
    fprintf(stderr, "(Error) main: Cannot open the database.\n");
    return 1;
  }
  
  if (signal(SIGTSTP, handle_SIGTSTP) == SIG_ERR) {
    ERROR("Error: cannot handle SIGTSTP signal");
  }

  pthread_mutex_init(&db_mutex, NULL);
  pthread_mutex_init(&queue_mutex, NULL);
  pthread_cond_init(&db_cond_variable, NULL);
	pthread_cond_init(&queue_cond_variable, NULL);
	
	//creating the consumer_threads
	int i;
	for (i = 0; i<THREADS; i++){
		if (pthread_create(&thread_consumers[i], NULL, incoming_request_handler_by_the_consumers, NULL) <0){
			ERROR("could not create consumer-thread");
			return EXIT_FAILURE;
		}
  }
  
  while (1) {
		int new_fd;
		// wait for incomming connection
		if ((new_fd = accept(socket_fd, (struct sockaddr *)&client_addr, &clen)) == -1) {
		  ERROR("accept()");
		}

		// got connection, serve request
		fprintf(stderr, "(Info) main: Got connection from!!! '%s'\n", inet_ntoa(client_addr.sin_addr));


		
		Queue_node *pointer_to_the_queue_node = NULL;
		pointer_to_the_queue_node = (Queue_node *)malloc(sizeof(Queue_node));

    struct timeval tv;
		gettimeofday(&tv, NULL);
    pointer_to_the_queue_node->connection_start_time = (tv.tv_sec + tv.tv_usec/1000000.0);
		pointer_to_the_queue_node->connection_socket_fd = new_fd;

		/*Check to see if Overwriting unread slot*/
    pthread_mutex_lock(&queue_mutex);
    while(total_entrances == QUEUE_SIZE){
      pthread_cond_wait(&queue_cond_variable, &queue_mutex);
    }
    
    if(tail == QUEUE_SIZE){
      tail = 0;
    }
		common_data_queue[tail] = pointer_to_the_queue_node; 
    tail += 1;
    total_entrances += 1;

    pthread_cond_broadcast(&queue_cond_variable);
		pthread_mutex_unlock(&queue_mutex);
	}
	
	
	return EXIT_SUCCESS;
}