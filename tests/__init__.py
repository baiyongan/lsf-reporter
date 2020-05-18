# -*- coding: utf-8 -*-
import os
import sys


print('home')
print(sys.argv[0])
print(os.path.join(sys.argv[0], 'home'))

import json

data = {'name': 'lilei', 'age': 30}
print(json.dumps(data, separators=(',', ':')))
print(json.dumps(data))

# return send_from_directory("{0}/{1}/{2}/".format(sys.path[0], storage_list[3], user_name),
#                        '{0}.xlsx'.format(query_bill_name), as_attachment=True)

