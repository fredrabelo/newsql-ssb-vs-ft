from statsmodels.stats.weightstats import ttest_ind
import numpy as np
import pingouin as pg
 
# code from https://www.geeksforgeeks.org/how-to-conduct-a-two-sample-t-test-in-python/
 
print("#####################################################")
print("############ T-TEST SS x FT OLAP X HTAP #############")
print("#####################################################")

# Creating data groups
htap_sf10_ss = np.array([274,312,439,654])
htap_sf10_ft = np.array([159,228,272,356])
 
# Conducting two-sample ttest
result_htap_sf10 = pg.ttest(htap_sf10_ss,htap_sf10_ft,correction=True)

# Print the result
print("HTAP SOLUTIONS SF=10 SS x FT")
print(result_htap_sf10)


htap_sf100_ss = np.array([2166,2794,3592,5910])
htap_sf100_ft = np.array([1304,1168,1288,1762])
result_htap_sf100 = pg.ttest(htap_sf100_ss,htap_sf100_ft,correction=True)
print("#####################################################")
print("HTAP SOLUTIONS SF=100 SS x FT")
print(result_htap_sf100)


##########################################################################

olap_sf10_ss = np.array([306,922,885,1212])
olap_sf10_ft = np.array([230,1879,3028,2267])
result_olap_sf10 = pg.ttest(olap_sf10_ss,olap_sf10_ft,correction=True)
print("#####################################################")
print("#####################################################")
print("OLAP SOLUTIONS SF=10 SS x FT")
print(result_olap_sf10)

olap_sf100_ss = np.array([2053,6453,7744,9825])
olap_sf100_ft = np.array([1314,11410,18320,14696])
result_olap_sf100 = pg.ttest(olap_sf100_ss,olap_sf100_ft,correction=True)
print("#####################################################")
print("OLAP SOLUTIONS SF=100 SS x FT")
print(result_olap_sf100)
print("#####################################################")
print("#####################################################")
