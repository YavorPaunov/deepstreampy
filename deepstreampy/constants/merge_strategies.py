def remote_wins(record, remote_value, remote_version, callback):
    callback(None, remote_value)


def local_wins(record, remote_value, remote_version, callback):
    callback(None, record.get())
