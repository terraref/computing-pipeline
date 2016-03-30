import time, json
import requests
from globusonline.transfer.api_client import TransferAPIClient, Transfer, APIError, goauth

f = "1M.dat" #"1G.dat"
glob_user = "maxzilla"
glob_pass = "TCiHdAslV3RnfEwD8kbj"
api_host = "http://localhost:5454"
metadata = {"mdone":100,"mdtwo":"greetings"}
dsname = "Gantry Test Dataset"


"""Send message to NCSA Globus monitor API that a new task has begun"""
def notifyMonitorOfNewTransfer(globusID, contents):
    print("Notifying monitor API")
    sess = requests.Session()
    sess.auth = (glob_user, glob_pass)
    sess.post(api_host+"/tasks", data=json.dumps({
        "user": glob_user, "globus_id": globusID, "contents": contents}))


print("Executing test transfer of "+f+" from Globus test endpoint to maxzilla local")
authToken = goauth.get_access_token(username=glob_user,password=glob_pass).token
api = TransferAPIClient(username="maxzilla", goauth=authToken)

# create submission id
status_code, status_message, submission_id = api.submission_id()
sid = submission_id['value']

# initiate transfer
t = Transfer(sid, "db57ddde-6d04-11e5-ba46-22000b92c6ec", "7d224c8c-ebaa-11e5-9829-22000b9da45e")
t.add_item('/data1/'+f, '/~/globus/'+f)
status_code, status_message, transfer_data = api.transfer(t)

# notify monitor to watch transfer
print("["+str(status_code)+"] "+status_message)
if status_code == 200 or status_code == 202:
    print(transfer_data['task_id'])
    dsobj = {dsname:
             {"files":
              {f:
               {"name":f,
                "md":metadata
                }
               },
              "md":{"dataset_md": "Yes", "another_key": "value"}
              }
             }
    notifyMonitorOfNewTransfer(transfer_data['task_id'], dsobj)
else:
    print("transfer initialization failed")
