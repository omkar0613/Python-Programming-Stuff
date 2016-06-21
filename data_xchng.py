
import numpy as np
from mpi4py import MPI


comm = MPI.COMM_WORLD
rank = comm.Get_rank()


if rank == 0:
   
   data = {'a': 7, 'b': 3.14}
   print "Status from server %r : \n sending data: %r " %(rank,data)  
   comm.send(data, dest=1, tag=11)
else:
   
   data = comm.recv(source=0, tag=11)
   print "Status from server %r : \n  received data: %r " %(rank,data)  
   

