#include <math.h>

static long long func_call_counter = 0;

// function to integrate
#pragma omp declare simd
double func (double x) {
  func_call_counter++;
  //  return pow(x,1.4)/3.1 - x/log(3.0);
    return 20.4 + pow(x,1.2)/3.1 - x/log(3.0);
}

long long get_func_call_count() {
  return func_call_counter;
}