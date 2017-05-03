f.check.shell =
  function( i ){
    file1 = paste0( "/home/data/NYCTaxis/trip_fare_",i,".csv" )
    shellcmd = paste0("cut -d, -f 1,4 ", file1 ," |sed 1d ")
    fare_m = system( shellcmd , intern =TRUE  )
    
    file2 = paste0( "/home/data/NYCTaxis/trip_data_",i,".csv" )
    shellcmd = paste0("cut -d, -f 1,6 ", file2, " |sed 1d " )
    data_m = system( shellcmd , intern =TRUE  )
    
    n =length( data_m)
    
    wrong = sum( !fare_m == data_m)
    return( c( wrong, n = n))
  }

library( parallel )
cl = makeCluster( 12, "FORK")
checkinfor = clusterApply( cl, seq(1, 12), f.check.shell )
temp = do.call( "rbind", checkinfor)
res = apply( temp, 2, sum)
res
