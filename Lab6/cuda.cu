#include "utility.h"
#include <stdio.h>
#include <stdlib.h>
#include <cuda_runtime.h>
#include <sys/time.h>

const int threadsinblock = 1024;

__host__
void errorexit(const char *s) {
    printf("\n%s", s);
    exit(EXIT_FAILURE);
}

__device__
bool is_prime(long num) {
  if (num <= 1) return false;
  if (num == 2) return true;
  if (num % 2 == 0) return false;
  for (long i = 3; i*i <= num; i += 2) {
    if (num % i == 0) {
      return false;
    }
  }
  return true;
}

__global__
void calculate(int *dresult, long upperLimit) {
  __shared__ int localPrimeCount;
  
  int tid = threadIdx.x;
  long globalId = blockIdx.x * blockDim.x + threadIdx.x;
  long candidate = globalId * 2 + 1;
  int isCandidatePrime = 0;

  if (tid == 0) {
    localPrimeCount = 0;
  }
  __syncthreads();

  if (candidate + 2 <= upperLimit) {
    if (is_prime(candidate) && is_prime(candidate + 2)) {
      isCandidatePrime = 1;
    }
  }
  if (isCandidatePrime) {
    atomicAdd(&localPrimeCount, 1);
  }
  __syncthreads();

  if (tid == 0 && localPrimeCount > 0) {
    atomicAdd(dresult, localPrimeCount);
  }
}

int main(int argc,char **argv) {

  Args ins__args;
  parseArgs(&ins__args, &argc, argv);
  
  //program input argument
  long inputArgument = ins__args.arg; 
  
  struct timeval ins__tstart, ins__tstop;
  gettimeofday(&ins__tstart, NULL);
  
  // run your CUDA kernel(s) here
  long  numOfThreads = inputArgument / 2; // skip even numbers
  int blocksingrid = numOfThreads / threadsinblock;
  if (numOfThreads % threadsinblock != 0) {
    blocksingrid++;
  }
  
  int *dresult = NULL;
  if (cudaSuccess != cudaMalloc((void**)&dresult, sizeof(int)))
    errorexit("Error allocating memory on the GPU");
  if (cudaSuccess != cudaMemset(dresult, 0, sizeof(int)))
    errorexit("Error initializing GPU memory");

  calculate<<<blocksingrid, threadsinblock>>>(dresult, inputArgument);

  if (cudaSuccess!=cudaGetLastError())
    errorexit("Error during kernel launch");

  int result = 0;
  if (cudaSuccess != cudaMemcpy(&result, dresult, sizeof(int), cudaMemcpyDeviceToHost))
    errorexit("There was an error while copying results from the GPU to the host");

  // synchronize/finalize your CUDA computations
  printf("Twin primes up to %ld: %d\n", inputArgument, result);
  cudaFree(dresult);
  gettimeofday(&ins__tstop, NULL);
  ins__printtime(&ins__tstart, &ins__tstop, ins__args.marker);
  printf("\n%s - Total execution time...", ins__args.marker);
}
