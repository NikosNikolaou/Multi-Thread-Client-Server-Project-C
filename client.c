/* client.c

   Sample code of 
   Assignment L1: Simple multi-threaded key-value server
   for the course MYY601 Operating Systems, University of Ioannina 

   (c) S. Anastasiadis, G. Kappes 2016

*/


#include "utils.h"
#include <stdlib.h>
#include <stdio.h>


#define SERVER_PORT     6767
#define BUF_SIZE        2048
#define MAXHOSTNAMELEN  1024
#define MAX_STATION_ID   128
#define ITER_COUNT          1
#define GET_MODE           1
#define PUT_MODE           2
#define USER_MODE          3


//Define client threads
#define CLIENT_THREADS 4

//Initialization of the client threads
pthread_t client_threads[CLIENT_THREADS];


//Initialization of the sending_request_to_server_fuction_parameter_passing structure
typedef struct sending_request_to_server_fuction_parameter_passing{
  char snd_buffer[BUF_SIZE];
  char *request;
  struct sockaddr_in server_addr;
  int count;
  int mode;
  int j;
  FILE *fp;
}Sending_request_to_server_fuction_parameter_passing;
/**
 * @name print_usage - Prints usage information.
 * @return
 */
void print_usage() {
  fprintf(stderr, "Usage: client [OPTION]...\n\n");
  fprintf(stderr, "Available Options:\n");
  fprintf(stderr, "-h:             Print this help message.\n");
  fprintf(stderr, "-a <address>:   Specify the server address or hostname.\n");
  fprintf(stderr, "-o <operation>: Send a single operation to the server.\n");
  fprintf(stderr, "                <operation>:\n");
  fprintf(stderr, "                PUT:key:value\n");
  fprintf(stderr, "                GET:key\n");
  fprintf(stderr, "-i <count>:     Specify the number of iterations.\n");
  fprintf(stderr, "-g:             Repeatedly send GET operations.\n");
  fprintf(stderr, "-p:             Repeatedly send PUT operations.\n");
}

/**
 * @name talk - Sends a message to the server and prints the response.
 * @server_addr: The server address.
 * @buffer: A buffer that contains a message for the server.
 *
 * @return
 */
void talk(const struct sockaddr_in server_addr, char *buffer, FILE *fp) {
  char rcv_buffer[BUF_SIZE];
  int socket_fd, numbytes;
      
  // create socket
  if ((socket_fd = socket(PF_INET, SOCK_STREAM, 0)) == -1) {
    ERROR("socket()");
  }

  // connect to the server.
  if (connect(socket_fd, (struct sockaddr*) &server_addr, sizeof(server_addr)) == -1) {
    ERROR("connect()");
  }
  
  // send message.
  write_str_to_socket(socket_fd, buffer, strlen(buffer));
      
  // receive results.
  printf("Result: ");
  do {
    memset(rcv_buffer, 0, BUF_SIZE);
    numbytes = read_str_from_socket(socket_fd, rcv_buffer, BUF_SIZE);
    if (numbytes != 0)
      printf("%s", rcv_buffer); // print to stdout
      fprintf(fp,"%s", rcv_buffer);
  } while (numbytes > 0);
  printf("\n");
  fprintf(fp, "\r\n");
      
  // close the connection to the server.
  close(socket_fd);
}

//The thread fuction
void *sending_request_to_server(void *parameters_struct){
  int station, value;
  Sending_request_to_server_fuction_parameter_passing *parameters = (Sending_request_to_server_fuction_parameter_passing *)parameters_struct;
  char buffer[100];
  sprintf(buffer, "%d", parameters->j);
  if(parameters->mode == GET_MODE){
    parameters->fp = fopen(strcat(buffer, "_GET_file.txt"), "w+");
  }else{
    parameters->fp = fopen(strcat(buffer, "_PUT_file.txt"), "w+");
  }
  

  if (parameters->mode == USER_MODE) {
    memset(parameters->snd_buffer, 0, BUF_SIZE);
    strncpy(parameters->snd_buffer, parameters->request, strlen(parameters->request));
    printf("Operation: %s\n", parameters->snd_buffer);
    talk(parameters->server_addr, parameters->snd_buffer, parameters->fp);
  } else {
    while(--parameters->count>=0) {
      for (station = 0; station <= MAX_STATION_ID; station++) {
        memset(parameters->snd_buffer, 0, BUF_SIZE);
        if (parameters->mode == GET_MODE) {
          // Repeatedly GET.
          sprintf(parameters->snd_buffer, "GET:station.%d.%d", station, parameters->j);
          fprintf(parameters->fp,"GET:station.%d.%d", station, parameters->j);
          fprintf(parameters->fp, "\r\n");
        } else if (parameters->mode == PUT_MODE) {
          // Repeatedly PUT.
          // create a random value.
          value = rand() % 65 + (-20);
          sprintf(parameters->snd_buffer, "PUT:station.%d.%d:%d", station, parameters->j, value);
          fprintf(parameters->fp,"PUT:station.%d.%d:%d", station, parameters->j, value);
          fprintf(parameters->fp, "\r\n");
        }
        printf("Operation: %s\n", parameters->snd_buffer);
        talk(parameters->server_addr, parameters->snd_buffer, parameters->fp);
      }
    }
  }
  fclose(parameters->fp);
  return EXIT_SUCCESS;
}

/**
 * @name main - The main routine.
 */
int main(int argc, char **argv) {
  char *host = NULL;
  char *request = NULL;
  int mode = 0;
  int option = 0;
  int count = ITER_COUNT;
  struct sockaddr_in server_addr;
  struct hostent *host_info;
  
  // Parse user parameters.
  while ((option = getopt(argc, argv,"i:hgpo:a:")) != -1) {
    switch (option) {
      case 'h':
        print_usage();
        exit(0);
      case 'a':
        host = optarg;
        break;
      case 'i':
        count = atoi(optarg);
	break;
      case 'g':
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = GET_MODE;
        break;
      case 'p': 
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -g, -p, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = PUT_MODE;
        break;
      case 'o':
        if (mode) {
          fprintf(stderr, "You can only specify one of the following: -r, -w, -o\n");
          exit(EXIT_FAILURE);
        }
        mode = USER_MODE;
        request = optarg;
        break;
      default:
        print_usage();
        exit(EXIT_FAILURE);
    }
  }

  // Check parameters.
  if (!mode) {
    fprintf(stderr, "Error: One of -g, -p, -o is required.\n\n");
    print_usage();
    exit(0);
  }
  if (!host) {
    fprintf(stderr, "Error: -a <address> is required.\n\n");
    print_usage();
    exit(0);
  }
  
  // get the host (server) info
  if ((host_info = gethostbyname(host)) == NULL) { 
    ERROR("gethostbyname()"); 
  }
    
  // create socket adress of server (type, IP-adress and port number)
  bzero(&server_addr, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr = *((struct in_addr*)host_info->h_addr);
  server_addr.sin_port = htons(SERVER_PORT);


  // create client threads and calling the thread function
  int i;
  for (i = 0; i<CLIENT_THREADS; i++){
    //Memory allocation and setting up the parameters
    Sending_request_to_server_fuction_parameter_passing *parameters = NULL;
    parameters = (Sending_request_to_server_fuction_parameter_passing *)malloc(sizeof(Sending_request_to_server_fuction_parameter_passing));
    parameters->mode = mode;
    parameters->request = request;
    parameters->server_addr = server_addr;
    parameters->count = count;
    parameters->j= i+1;
    if (pthread_create(&client_threads[i], NULL, sending_request_to_server, (void *)parameters) <0){
			ERROR("could not create client-threads");
			return EXIT_FAILURE;
		}
  }

  // Joining the threads
  for(i = 0; i<CLIENT_THREADS; i++){
		pthread_join(client_threads[i], NULL);
    }

  return 0;
}

