from unittest.mock import Mock


def main():
    mock = Mock()

    import json
    data = json.dumps({'a': 1})
    json = mock
    print(dir(json))
    pass


if __name__ == '__main__':
    main()
