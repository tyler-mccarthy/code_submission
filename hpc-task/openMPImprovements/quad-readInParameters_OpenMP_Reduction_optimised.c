/*
 * Code to numerically integrate ("quadrature") function in file compiled with this that contains 
 *   function func(double x)
 * from a to b using numberQuads, where
 * a,b, numberQuads 
 * given on command line
 * 
 * integral = sum of areas trapezoidals that approximate curve
 * area of trapezoidal = mean height * width
 *
 * Version: OpenMP, Reduction with SIMD
 * (c) michael k bane, tyler mccarthy
 *
 *
 * v2.0 (22Apr2025)
 */

#include <stdio.h>
#include <stdlib.h>
#include <omp.h>  // OpenMP prototypes
#include "func_utils.h"

double func(double x);      // prototype for func to integrate (separate file)

int main(int argc, char* argv[]) {
  double a, b;  /* bounds */
  long long numberQuads;
  double width, halfW;
  double integrand;

  
  /* OpenMP vars for timing */
  double wallStart, wallEnd;
  
  // OpenMP variables
  int myThread, numPEs;
  
  // read in from command line what to integrate
  // command line = exeName and 3 parameters (a, b, numberQuads) i.e. expect 4 items
  if (argc < 4) {
    printf("Need to pass bounds ('a','b') and numberQuads\n");
  }
  else {
    a = atof(argv[1]); // see 'man atof'
    b = atof(argv[2]); 
    numberQuads = atoll(argv[3]);
    printf("Integrating from a=%f to b=%f using %d trapezoidals\n",a,b,numberQuads);
    integrand = 0.0;
    width = (b-a) / (double) numberQuads;
    halfW = width / 2.0;
    
    wallStart = omp_get_wtime();
    
    // OpenMP implementation based on this being a Reduction problen
    // shared: variables only read from
    // private: variables with temp values (within par reg) with >1 thread updating
    // then consider var "numPEs". Put as shared so have access after parallel region
    // we are reducing partial values of integrand (on each thread) to global value of integrand (on controller thread)
    #pragma omp parallel default(none) \
    shared(numberQuads, a, width, halfW)	 \
    private(myThread) \
    shared(numPEs) \
    reduction(+: integrand)
    {
      double local = 0.0;
      double fL, fR;
      double xL, xR;
      int first = 1;              /* loop counter */

      myThread = omp_get_thread_num();
      numPEs = omp_get_num_threads();  // no race condition since same value would be written by each thread
      # pragma omp for schedule(static)
      for (long long i = 0; i < numberQuads; i++) {
        xL = a + i * width;

        if (first) {
          fL = func(xL);
          first = 0;
        } else {
          fL = fR;
        }
        xR = xL + width;
        fR = func(xR);

        local += (fL + fR) * halfW;
      }
      integrand += local;
    }
    double wallEnd = omp_get_wtime();
    printf("WALL CLOCK Time: %.6f seconds\n", wallEnd - wallStart);
    printf("(Openmp [Reduce] version using %d threads): integral = %f\n", numPEs, integrand);
    double WALLtimeTaken = wallEnd - wallStart;
    printf("WALL CLOCK Time: %f seconds  \n", WALLtimeTaken);
    printf("FUNC CALL COUNT: %lld\n", get_func_call_count());
  }
}

