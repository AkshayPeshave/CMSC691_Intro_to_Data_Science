import math, tabulate

points = {'A1':(2,10), 'A2':(2,5), 'A3':(8,4), 'B1':(5,8),'B2':(7,5), 'B3':(6,4), 'C1':(1,2),'C2':(4,9)}

centroids=[(2,10),(5,8),(1,2)]

distMtx = [] 
for point, coordinates in points.iteritems():
	distVec=[point]
	for centroid in centroids:
		dist=math.pow(coordinates[0]*1.0-centroid[0],2) + math.pow(coordinates[1]-centroid[1],2)
		dist=math.sqrt(dist)
		distVec.append(dist)
	distMtx.append(distVec)

print(tabulate.tabulate(distMtx))
print()

centroids=[(2,10),(6,6),(1.5,3.5)]

distMtx = [] 
for point, coordinates in points.iteritems():
	distVec=[point]
	for centroid in centroids:
		dist=math.pow(coordinates[0]*1.0-centroid[0],2) + math.pow(coordinates[1]-centroid[1],2)
		dist=math.sqrt(dist)
		distVec.append(dist)
	distMtx.append(distVec)

print(tabulate.tabulate(distMtx))

centroids=[(3,9.5),(6.5,5.25),(1.5,3.5)]

distMtx = [] 
for point, coordinates in points.iteritems():
	distVec=[point]
	for centroid in centroids:
		dist=math.pow(coordinates[0]*1.0-centroid[0],2) + math.pow(coordinates[1]-centroid[1],2)
		dist=math.sqrt(dist)
		distVec.append(dist)
	distMtx.append(distVec)

print(tabulate.tabulate(distMtx))


centroids=[(3.67,9),(7,4.33),(1.5,3.5)]

distMtx = [] 
for point, coordinates in points.iteritems():
	distVec=[point]
	for centroid in centroids:
		dist=math.pow(coordinates[0]*1.0-centroid[0],2) + math.pow(coordinates[1]-centroid[1],2)
		dist=math.sqrt(dist)
		distVec.append(dist)
	distMtx.append(distVec)

print(tabulate.tabulate(distMtx))



