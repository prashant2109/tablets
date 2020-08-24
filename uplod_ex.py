class upload:
    def __inti__(self):
        pass

    def upload_excel(self, ijson):
        import muthu_translate.socket_client_utils2 as up
        up_obj = up.socket_client_utils2('172.16.20.229', '6666') 
        data  = up_obj.send_socket(ijson)
        print data
         
        

if __name__ == '__main__':
    obj = upload()
    ijson = {"path":"/var/www/html/muthu/Amazon_f.xlsx"}
    res = obj.upload_excel(ijson)
