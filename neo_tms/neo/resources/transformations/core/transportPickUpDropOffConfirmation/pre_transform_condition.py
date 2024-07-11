#Checks if document action code is "CHANGE_BY_REFRESH"
def check_document_action_code_CBR(*args):
    if args[0][0]=="CHANGE_BY_REFRESH":
        return True
    return False

#Checks if document action code is "DELETE"
def check_document_action_code_del(*args):
    if args[0][0]=="DELETE":
        return True
    return False