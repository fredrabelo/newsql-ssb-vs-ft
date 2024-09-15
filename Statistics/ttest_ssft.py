from statsmodels.stats.weightstats import ttest_ind
import numpy as np
import pingouin as pg
 
# code from https://www.geeksforgeeks.org/how-to-conduct-a-two-sample-t-test-in-python/

 
print("#####################################################")
print("############ T-TEST SS x FT PER SOLUTION ############")
print("#####################################################")

# Creating data groups
singlestore_sf10_ss = np.array([674,156,85,190,169,164,254,195,181,173,340,1191,1073])
singlestore_sf10_ft = np.array([93,113,80,120,117,88,136,135,90,81,132,167,143])
 
# Conducting two-sample ttest
result_singlestore_sf10 = pg.ttest(singlestore_sf10_ss,singlestore_sf10_ft,correction=True)

# Print the result
print("SINGLESTORE SF=10 SS x FT")
print(result_singlestore_sf10)


singlestore_sf100_ss = np.array([7229,1459,907,2479,2357,2613,3159,2394,1601,1524,3529,12536,11842])
singlestore_sf100_ft = np.array([942,1205,809,1225,1102,910,1379,1344,862,762,1432,1723,1255])
result_singlestore_sf100 = pg.ttest(singlestore_sf100_ss,singlestore_sf100_ft,correction=True)
print("#####################################################")
print("SINGLESTORE SF=100 SS x FT")
print(result_singlestore_sf100)

##########################################################################

tidb_sf10_ss = np.array([344,312,313,563,485,250,656,625,672,984,765,531,640])
tidb_sf10_ft = np.array([235,250,250,421,391,359,547,609,375,438,594,656,687])
result_tidb_sf10 = pg.ttest(tidb_sf10_ss,tidb_sf10_ft,correction=True)
print("#####################################################")
print("#####################################################")
print("TIDB SF=10 SS x FT")
print(result_tidb_sf10)

##########################################################################

db2_sf10_ss = np.array([257,187,136,374,321,290,598,412,364,157,590,434,326])
db2_sf10_ft = np.array([143,131,134,200,194,164,310,225,196,127,306,291,232])
result_db2_sf10 = pg.ttest(db2_sf10_ss,db2_sf10_ft,correction=True)
print("#####################################################")
print("#####################################################")
print("IBM DB2 SF=10 SS x FT")
print(result_db2_sf10)

db2_sf100_ss = np.array([2126,1726,1211,3929,3133,2556,4944,3311,2514,908,6441,3936,2691])
db2_sf100_ft = np.array([2069,1587,1212,1556,1218,996,2151,1547,1139,1118,2477,2138,1545])
result_db2_sf100 = pg.ttest(db2_sf100_ss,db2_sf100_ft,correction=True)
print("#####################################################")
print("IBM DB2 SF=100 SS x FT")
print(result_db2_sf100)

##########################################################################

redshift_sf10_ss = np.array([147,124,115,277,244,230,455,249,227,218,553,472,472])
redshift_sf10_ft = np.array([89,70,77,1016,1020,824,1632,1597,812,1151,1657,1739,1595])
result_redshift_sf10 = pg.ttest(redshift_sf10_ss,redshift_sf10_ft,correction=True)
print("#####################################################")
print("#####################################################")
print("REDSHIFT SF=10 SS x FT")
print(result_redshift_sf10)

redshift_sf100_ss = np.array([1298,1070,1015,2184,2995,1728,4876,2385,1921,1802,5054,4292,3281])
redshift_sf100_ft = np.array([679,512,705,9839,10106,8002,16378,16367,8079,10943,16816,16653,16635])
result_redshift_sf100 = pg.ttest(redshift_sf100_ss,redshift_sf100_ft,correction=True)
print("#####################################################")
print("REDSHIFT SF=100 SS x FT")
print(result_redshift_sf100)

##########################################################################

mariadb_sf10_ss = np.array([568,261,248,2468,1957,1191,2856,1815,1174,1010,3300,1968,1465])
mariadb_sf10_ft = np.array([389,226,233,4163,4279,4078,6362,5886,7989,8763,9237,2720,1868])
result_mariadb_sf10 = pg.ttest(mariadb_sf10_ss,mariadb_sf10_ft,correction=True)
print("#####################################################")
print("#####################################################")
print("MARIADB SF=10 SS x FT")
print(result_mariadb_sf10)

mariadb_sf100_ss = np.array([4625,2357,2287,18345,14934,8743,28593,16341,13605,7917,31256,16223,11968])
mariadb_sf100_ft = np.array([2242,1418,1466,21475,21788,20900,33457,31073,41804,45993,47260,14257,9982])
result_mariadb_sf100 = pg.ttest(mariadb_sf100_ss,mariadb_sf100_ft,correction=True)
print("#####################################################")
print("MARIADB SF=100 SS x FT")
print(result_mariadb_sf100)

##########################################################################

snowflake_sf10_ss = np.array([507,420,366,703,617,613,824,575,626,572,989,941,752])
snowflake_sf10_ft = np.array([369,322,292,453,653,424,610,481,538,511,567,523,497])
result_snowflake_sf10 = pg.ttest(snowflake_sf10_ss,snowflake_sf10_ft,correction=True)
print("#####################################################")
print("#####################################################")
print("SNOWFLAKE SF=10 SS x FT")
print(result_snowflake_sf10)

snowflake_sf100_ss = np.array([1909,2155,1757,3337,3367,2439,5903,3755,3115,2713,6482,5234,4640])
snowflake_sf100_ft = np.array([1555,1694,1558,2934,4780,2862,4382,3179,3512,4674,4054,3537,3073])
result_snowflake_sf100 = pg.ttest(snowflake_sf100_ss,snowflake_sf100_ft,correction=True)
print("#####################################################")
print("SNOWFLAKE SF=100 SS x FT")
print(result_snowflake_sf100)
print("#####################################################")
print("#####################################################")
