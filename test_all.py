
import numpy as np
import socket
from mpi4py import MPI



comm = MPI.COMM_WORLD
rank = comm.Get_rank()
hostName = socket.gethostname()
print " Server with hostname %r and of rank %r is up and running !!"  %(hostName,rank) 
