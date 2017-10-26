import os
import happybase
import time

filepath=[]
afterline=[]
count=0
for root, dirs, files in os.walk("log"):
	for f in files:
		count+=1
		filepath.append(os.path.join(root, f))
		temp=f.split('_')
		templist=[]
		templist.append(temp[0])
		if count==8:
			afterline.append(templist)
			count=0
		
filenum=0
connection = happybase.Connection('localhost')
connection.open()
table = connection.table('data')
batch = table.batch()

for file in filepath:
	rowcount=0
	count+=1
	f = open(file,'r')
	print ("Connected to file. name: %s" % (file))
	for line in f.readlines(): 
		line=line.strip()
		line=line.split(',')
		afterline[filenum].append(line[1])
		rowcount+=1

	if(count == 8):
		filenum+=1 

for i in range(filenum+1):
	for j in range(1,rowcount+1):
		batch.put(afterline[i][0]+"--"+str(j), { "displacement:sensor_0": afterline[i][j], "displacement:sensor_1": afterline[i][j+rowcount],\
		"displacement:sensor_2": afterline[i][j+2*rowcount], "displacement:sensor_3": afterline[i][j+3*rowcount],\
		"displacement:sensor_4": afterline[i][j+4*rowcount], "displacement:sensor_5": afterline[i][j+5*rowcount],\
		"displacement:sensor_6": afterline[i][j+6*rowcount], "displacement:sensor_7": afterline[i][j+7*rowcount]})
batch.send()
connection.close()
duration = time.time() - start_time
print ("Done. row count: %i, duration: %.3f s" % (8*rowcount, duration))
#rowcount=8711