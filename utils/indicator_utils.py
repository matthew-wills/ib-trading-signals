def IBR(H,L,C):
    ans = (C-L)/(H-L)
    return(ans)

def ROC(data, p1):
    roc = 100 * ((data.iloc[-1] - data.iloc[-p1 - 1]) / data.iloc[-p1 - 1])
    return round(roc, 2)

def tickSize(l):
    tick = 0.01
    if l < 2:
        tick = 0.005
    if l < 0.1:
        tick = 0.001
    return(tick)

