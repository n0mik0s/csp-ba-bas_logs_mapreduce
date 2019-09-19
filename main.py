import os
from requests_kerberos import HTTPKerberosAuth
from hdfs.ext.kerberos import KerberosClient

url = "http://alderamin.sdab.sn:50070;http://fomalgaut.sdab.sn:50070"
os.environ["KRB5_CLIENT_KTNAME"] = "/home/doopy/pyprojects/csp-ba-bas_logs_mapreduce/doopy.keytab"
kerberos_auth = HTTPKerberosAuth(principal="doopy@ITM.SN")

client = KerberosClient(url)
print(client.list('/tmp/'))