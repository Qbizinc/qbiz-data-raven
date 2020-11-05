

def load_template(path):
    with open(path, 'r') as infile:
        template = infile.read()
    infile.close()
    return template


def test_reuslt_msg_template():
    path = "dataraven/static/test_result_message_template.txt"
    return load_template(path)


def hard_fail_msg_template():
    path = "dataraven/static/hard_fail_message_template.txt"
    return load_template(path)