class MainClassFA():
    
    def __init__(self):
        self.data
        self.count_basis = "weekly"
        
    @GetProperty
    def get_data(self):
        return self.data
    
    @SetProperty
    def set_data(self, data):
        self.data = data
    
    def form_tick_bars():
        # ETF trick roll
        # Count bars using count_basis
        
    def form_volume_bars():
        # ETF trick roll
        # Count bars using count_basis
    def form_dollar_bars():
        # ETF trick roll
        # Count bars using count_basis
    
    def form_dollar_imbalance_bars():
        
    def plot_bars(bar_type):
        
    def compute_serial_corr():
    
    def partition_subset(subsetType='monthly'):
        
    def compute_var_of_returns():
        
    def compute_var_vars():
        
    def jarque_bera_test():
        
    def pcaWeights(self, cov, riskDist=None, riskTarget=1.):
        # Following the riskAlloc distribution, match riskTarget
        eVal, eVec=np.eigh(cov) # must be Hermitian
        indices=eVal.argsort()[::-1] # arguments for sorting eVal desc
        eVal, eVec=eVal[indices],eVec[:,indices]
        if riskDist is None:
            riskDist=np-zeros(cov.shape[0])
            riskDist[-1]=1.
        loads=riskTarget*(riskDist/eVal)**.5
        wghts=np.dot(eVec,np.reshape(loads, (-1,1)))
        #ctr=(loads/riskTarget)**2*eVal # verify riskDist
        return wghts
    
    def getRolledSeries(self, pathIn,key):
        series=pd.read_hdf(pathIn,key='bars/ES_10k')
        series['Time']=pd.to_datetime(series['Time'],format='%Y%m%d%H%M%S%f')
        series=series.set_index('Time')
        gaps=rollGaps(series)
        for fld in ['Close', 'VWAP']:series[fld]-=gaps
        return series
    
    def rollGaps(series,dictio={'Instrument':'FUT_CUR_GEN_TICKER','Open':'PX_OPEN','Close':'PX_LAST'},matchEnd=True):
        # Compute gaps at each roll, between previous close and next open
        rollDates=series[dictio['Instrument']].drop_duplicates(keep='first').index
        gaps=series[dictio['Close']]*0
        iloc=list(series.index)
        iloc=[iloc.index(i)-1 for i in rollDates] # index of days prior to roll
        gaps.loc[rollDates[1:]]=series[dictio['Open']].loc[rollDates[1:]] - \
            series[dictio['Close']].iloc[iloc[1:]].values
        gaps=gaps.cumsum()
        if matchEnd:gaps-=gaps.iloc[-1] # roll backward
        return gaps
    
    def test_rollGaps(df):
        raw = df
        gaps=rollGaps(raw,dictio={'Instrument':'Book','Open':'open','Close':'close'})
        rolled=raw.copy(deep=True)
        for fld in ['Open','Close']:rolled[fld]-=gaps
        rolled['Return']=rolled['Close'].diff()/raw['Close'].shift(1)
        rolled['rPrices']=(1+rolled['Returns']).cumprod()
        
    def getTEvents(gRaw,h):
        tEvents,sPos,sNeg=[],0,0
        diff=gRaw.diff()
        for i in diff.index[1:]:
            sPos,sNeg=max(0,sPos+diff.loc[i]),min(0,sNeg+diff.loc[i])
            if sNeg<-h:
                sNeg=0;tEvents.append(i)
            elif sPos>h:
                sPos=0;tEvents.append(i)
        return pd.DatetimeIndex(tEvents)