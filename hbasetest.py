import os
import happybase
import time
filepath=[]
for root, dirs, files in os.walk("log"):
	for f in files:
		filepath.append(os.path.join(root, f))
connection = happybase.Connection('localhost')
connection.open()
table = connection.table('data')
batch = table.batch()
for file in filepath:
	f = open(file,'r')
	print ("Connected to file. name: %s" % (file))
	for line in f.readlines(): 
		row_count +=1
		line=line.strip()
		line=line.split(',')
		batch.put(line[0], { "displacement:X": line[1], "displacement:Y": line[2], "displacement:Z": line[3]})
batch.send()
connection.close()
duration = time.time() - start_time
print ("Done. row count: %i, duration: %.3f s" % (row_count, duration))
