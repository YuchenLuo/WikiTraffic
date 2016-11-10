import numpy as np
import matplotlib.pyplot as plt
import re
import scipy.fftpack

from datetime import datetime

def autocorr(x):
    acx = np.concatenate((x,x,x))
    ac = np.correlate( x, acx, "same" )
    return ac[x.size:2*x.size]
def fftplt( fig, ft ):
    N = ft.size
    # sample spacing
    T = 1
    x = np.linspace(0.0, N*T, N)
    y = ft
    yf = scipy.fftpack.fft(y)
    xf = np.linspace(0.0, 1.0/(2.0*T), N/2)

    yf[0]= 0
    fig.plot(x[:N/2], 2.0/N * np.abs(yf[:N/2]),'r-')
    #fig.plot( yf )
    #print yf
    
    #fftx = np.fft.fft(x)
    #fftx[0] = 0
    #return fftx
def ptfilter(a,q):
    peaks = []
    p = np.percentile(a,q)
    x = []
    num = 0
    i = 0
    for n in a:
        if( n > p ):
            peaks.append(i)
            x.append(n)
            num = num + 1
        else:
            x.append(0)
        i += 1            
    print "percentile", p, "num", num, "peaks", peaks
    return x, peaks

def plotResults():
    #print self.state
    #hist = plt.figure(2)
    #count, bins, ignored = plt.hist( self.load, 20, normed=True)
    #hist.show()
    fig = plt.figure()
    ax1 = fig.add_subplot(311)
    ax1.plot( pc, 'b-')
    ax1.set_xlabel('Time (Hours)')
    ax1.set_ylabel('Page Views', color='b')
    ax1.set_title('Hourly Page Views')
    for tl in ax1.get_yticklabels():
        tl.set_color('b')

    #ax2 = ax1.twinx()
    x = np.array( pc, dtype=np.int64)
    #ax2.plot( g1, 'r-')        
    #ax2.set_ylabel('state', color='r')
    #for tl in ax2.get_yticklabels():
    #    tl.set_color('r')

    sp2 = fig.add_subplot(312)
    sp2.set_title('Autocorrelation')
    ac = autocorr(x) 
    sp2.plot( ac )
    
    sp3 = fig.add_subplot(313)
    sp3.set_title('Frequency')    
    fftplt(sp3,ac)
    
    fig2 = plt.figure(2)
    plt.title('Load Spikes and Outages')
    plt.plot( np.abs(g1), 'b-' )    
    plt.plot( perFiltered,'r-' )

    fig3 = plt.figure(3)
    f3ax1 = fig3.add_subplot(111)
    f3ax1.plot( pc, 'b-')
    f3ax1.set_xlabel('Time (Hours)')
    f3ax1.set_ylabel('Page Views', color='b')
    f3ax1.set_title('Hourly Page Views')
    plt.show()

pc = []
bw = []

f = open('log.txt', 'r')
fpc = open('wiki.data.txt', 'w')
fpcc = open('wiki.class.data.txt', 'w')
#fbw = open('bw.data.txt', 'w')

for line in f:
    row = line.split()
    timestr = row[0]
    timestr = re.sub('/mnt/data/wikidata/wikistats/pagecounts/pagecounts-','',timestr)
    timestr = re.sub('.gz','',timestr)
    
    y = int( timestr[:4] )
    m = int( timestr[4:6] )
    d = int( timestr[6:8] )
    h = int( timestr[9:11] )
    mins = int( timestr[11:13] )
    s = int( timestr[13:15] )

    dt = datetime(y,m,d,h,mins,s)

    unixtime = str( dt.strftime("%s") );
    #print timestr,"|", unixtime, row[1], row[2]
    pc.append(row[1])
    bw.append(row[2])

# derived metrics
windowsize=48
offset = 24
g1 = np.gradient( np.array( pc, dtype=np.int64) )
res = ptfilter( np.abs(g1), 99.5  )
perFiltered = res[0]
pks=res[1]
print "lines:", len(pc)

total = 0 
classcount = 0

for i in range( windowsize+offset, len(pc) ):
    total += 1
    line=""
    idx = 0
    for j in range( windowsize ):
        idx+=1
        line += str( idx )+":"+str(pc[i-j-offset])+" "
    for j in range( len(pks) ):
        idx+=1
        line += str( idx )+":"+str(pks[j])+" "
        
    line += "\n"
    #regression
    label = str( pc[i] ) + " ";
    fpc.write( label + line )
    #classification
    if( perFiltered[i] != 0 ):
        label = "1 "
        classcount += 1
        for n in range(500):
            fpcc.write( label + line )       
    else:
        label = "0 "
    fpcc.write( label + line )

print "total", total
print "classcount", classcount

f.close()
fpc.close()
#fbw.close()
fpcc.close()
plotResults()




