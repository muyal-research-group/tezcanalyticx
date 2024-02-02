import pyarrow as pa
import pyarrow.compute as pc
# from pyarrow.lib import 
# pa.Array
accesses = pa.array([1,2,201,1,2,4,4,2,4,102])
total_access = accesses.sum()
print(total_access)
freq = pc.multiply(accesses,1/total_access.as_py())
print(freq)
    