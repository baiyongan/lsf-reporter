# -*- coding: utf-8 -*-

import io
import json
from django.http import HttpResponse, FileResponse

class BillTransfer():
    """
    传递账单文件到前端
    """

    def GetExcelStream(self, file):
        excel_stream = io.BytesIO()
        file.save(excel_stream)  # 传给save函数的不是保存文件名，而是一个BytesIO流（在内存中读写）
        res = excel_stream.getvalue()  # getvalue方法用于获得写入后的byte将结果返回给re
        excel_stream.close()
        return  res


    def ExcelDownlaod(self):
        file=open('../../doc/test/Target.xlsx', 'rb')
        response =FileResponse(file)
        response['Content-Type']='application/octet-stream'
        response['Content-Disposition']='attachment; filename="Target.xlsx"'
        return response

if __name__ == "__main__":


    bt = BillTransfer()
    bt.ExcelDownlaod()