import os
import zipfile
import shutil
import urllib
import boto3

TMP_DIR = '/tmp'
clients = {
    'lambda': None,
    's3': None
}
ENV_VARIABLES = [
    'SOURCE_BUCKET',
    'BUILD_BUCKET'
]
response_body = {
    'status': 'script executed without exception'
}
ACTION_TYPE = 'layer_update'


def lambda_handler(event, context):
    global response_body, LAYER_NAME, ACTION_TYPE, VERSION_TAG

    response_body['environment'] = {}
    for v in ENV_VARIABLES:
        globals()[v] = os.environ[v]
        response_body['environment'][v] = os.environ[v]

    response_body['request'] = {}
    bucket_context = read_bucket_context(event)
    print(f'found bucket context: {bucket_context}')
    if bucket_context:
        ACTION_TYPE = 'layer_update'
        event_bucket = bucket_context['event_bucket']
        LAYER_NAME = bucket_context['layer_name']
        package_name = bucket_context['package_name']
        response_body['request']['event_bucket'] = event_bucket
        response_body['request']['LAYER_NAME'] = LAYER_NAME
        response_body['request']['package_name'] = package_name
    else:
        for a in event:
            globals()[a] = event[a]
            response_body['request'][a] = event[a]

    response_body['ACTION_TYPE'] = ACTION_TYPE
    if ACTION_TYPE == 'read_into_s3':
        read_into_s3(
            LAYER_NAME,
            LAYER_VERSION,
            package_name=LAYER_NAME,
            package_dir=f'python/{LAYER_NAME}'
        )
    elif (ACTION_TYPE == 'layer_update') and event_bucket == SOURCE_BUCKET:
        print(f'calling layer_update with args layer_name:{LAYER_NAME}, package_name:{package_name}')
        layer_update(
            LAYER_NAME,
            package_name,
            f'python/{LAYER_NAME}'
        )

    print(f'lambda execution completed. results {response_body}')

    return {
        'statusCode': 200,
        'body': response_body
    }


def read_bucket_context(event):
    bucket_context = {}
    if 'Records' in event:
        if 's3' in event['Records'][0]:
            bucket_details = event['Records'][0]['s3']['bucket']
            object_details = event['Records'][0]['s3']['object']
            bucket_context['event_bucket'] = bucket_details['name']
            layer_name = object_details['key'].split('/')[0]
            package_name = object_details['key'].split('/')[1].replace('.zip', '')
            bucket_context['layer_name'] = layer_name
            bucket_context['package_name'] = package_name
    return bucket_context


def client_load(service):
    global clients
    if service in ['lambda', 's3']:
        clients[service] = boto3.client(service)


def client_unload(service):
    global clients
    clients[service] = None


def download_file(url, local_path):
    with urllib.request.urlopen(url) as response, open(local_path, 'wb') as out_file:
        out_file.write(response.read())


def layer_update(layer_name, package_name, package_dir):
    layer_zip_path = f'{TMP_DIR}/layer.zip'
    layer_key = f'{package_name}.zip'

    package_extract_dir = f'{TMP_DIR}/{package_name}'
    package_zip_path = f'{TMP_DIR}/{package_name}.zip'
    package_key = f'{layer_name}/{package_name}.zip'
    dependencies_extract_dir = f'{TMP_DIR}/dependencies'
    dependencies_zip_path = f'{TMP_DIR}/dependencies.zip'
    dependencies_key = f'{layer_name}/dependencies.zip'

    print(f'downloading files with package_key:{package_key}, package_zip_path:{package_zip_path}')

    client_load('s3')

    clients['s3'].download_file(SOURCE_BUCKET, package_key, package_zip_path)
    clients['s3'].download_file(SOURCE_BUCKET, dependencies_key, dependencies_zip_path)

    with zipfile.ZipFile(layer_zip_path, 'w') as layer_zip:
        package_count = 0
        with zipfile.ZipFile(package_zip_path, 'r') as package_zip:
            for item in package_zip.infolist():
                item_filename = os.path.join(package_dir, item.filename)
                extracted_path = os.path.join(package_extract_dir, item.filename)
                item_extracted = package_zip.extract(item, path=package_extract_dir)
                # item_info = {'extracted_path': extracted_path, 'filename': item.filename, 'item_filepath': item_filename}
                # print(('item info', item_info))
                layer_zip.write(item_extracted, arcname=item_filename)
                package_count = package_count + 1

        dependencies_count = 0
        with zipfile.ZipFile(dependencies_zip_path, 'r') as dependencies_zip:
            for item in dependencies_zip.infolist():
                item_extracted = dependencies_zip.extract(item, path=dependencies_extract_dir)
                layer_zip.write(item_extracted, arcname=item.filename)
                dependencies_count = dependencies_count + 1

    response_body['package'] = {'path': package_key, 'files': package_count}
    response_body['dependencies'] = {'path': dependencies_key, 'files': dependencies_count}

    clients['s3'].upload_file(layer_zip_path, BUILD_BUCKET, layer_key)

    response_body['s3 upload'] = {'status': 'success', 'bucket': BUILD_BUCKET, 'layer': layer_name, 'key': layer_key}

    client_unload('s3')

    shutil.rmtree(TMP_DIR, ignore_errors=True)
    response_body['tmp_dir_status'] = 'tmp dir cleaned.'


def read_into_s3(layer_name, layer_version, package_name='', package_dir=''):
    download_path = f'{TMP_DIR}/payload.zip'
    extract_path = f'{TMP_DIR}/{layer_name}-{layer_version}'

    package_extract = package_name != ''
    if package_extract:
        package_zip_path = f'{TMP_DIR}/{package_name}.zip'
        dependencies_zip_path = f'{TMP_DIR}/dependencies.zip'
        package_key = f'{layer_name}/{package_name}.zip'
        dependencies_key = f'{layer_name}/dependencies.zip'
    else:
        layer_zip_path = f'{TMP_DIR}/{layer_name}.zip'
        layer_key = f'{layer_name}.zip'

    client_load('lambda')

    layer_info = clients['lambda'].get_layer_version(
        LayerName=layer_name,
        VersionNumber=int(layer_version)
    )
    download_url = layer_info['Content']['Location']
    response_body['download_url'] = download_url[:20]

    client_unload('lambda')

    download_file(download_url, download_path)
    response_body['download'] = {'status': 'success', 'path': download_path}

    with zipfile.ZipFile(download_path, 'r') as zip_ref:
        os.makedirs(extract_path, exist_ok=True)
        zip_ref.extractall(extract_path)

    if package_extract:
        file_count = 0
        with zipfile.ZipFile(package_zip_path, 'w') as package_zip:
            package_folder = os.path.join(extract_path, package_dir)
            for foldername, subfolders, filenames in os.walk(package_folder):
                for filename in filenames:
                    file_path = os.path.join(foldername, filename)
                    arcname = os.path.relpath(file_path, package_folder)
                    package_zip.write(file_path, arcname)
                    file_count = file_count + 1

        response_body['package_zip'] = {'path': package_zip_path, 'source_folder': package_folder, 'files': file_count}

    if package_extract:
        response_key = 'dependencies'
        zip_path = dependencies_zip_path
        s3_key = dependencies_key
    else:
        response_key = 'layer'
        zip_path = layer_zip_path
        s3_key = layer_key

    file_count = 0
    with zipfile.ZipFile(zip_path, 'w') as zip_file:
        for root, dirs, files in os.walk(extract_path):
            if root != package_folder or not package_extract:
                for filename in files:
                    file_path = os.path.join(root, filename)
                    arcname = os.path.relpath(file_path, extract_path)
                    zip_file.write(file_path, arcname)
                    file_count = file_count + 1

    response_body[response_key] = {'path': zip_path, 'source_folder': extract_path, 'files': file_count}

    client_load('s3')

    clients['s3'].upload_file(package_zip_path, SOURCE_BUCKET, package_key)
    clients['s3'].upload_file(zip_path, SOURCE_BUCKET, s3_key)

    client_unload('s3')

    response_body['s3 upload'] = {'status': 'success', 'bucket': S3_BUCKET, 'package': package_key,
                                  response_key: s3_key}

    os.remove(download_path)
    shutil.rmtree(TMP_DIR, ignore_errors=True)
    response_body['tmp_dir_status'] = 'tmp dir cleaned.'