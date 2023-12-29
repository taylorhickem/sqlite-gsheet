# layer_manager
This lambda function builds a custom python library
for sqlgsheet that can be consumed by other applications.
It includes the dependencies libraries that sqlghseet
depends on listed in the requirements.txt file. 
The building is performed on AWS resources by combining 
dependencies and sqlgsheet source code from different AWS S3 bucket locations
and output into the AWS S3 layer bucket location.

The lambda function is parameterized so that it can be used
by any package that follows the same dependencies pattern as the sqlgsheet use case.

All files are *.zip file format
```
dependencies (source S3) + sqlgsheet (source S3) -> layer package ( layer S3)
```
## S3 bucket locations

_from: source bucket_
```
/<source bucket>
    /<package>
        dependencies.zip
        <package>-<version>.zip # package source code
```

_to: layer bucket_
```
/<layer bucket>
    <package>-<version>.zip # layer package including dependencies
```

## layer package
The layer package is a *.zip file with one main *python* directory
which contains all of the dependencies for the package + the source code for the package
included in the same main *python* directory

```
/python
    ... # other libraries that the package depends on
    /<package>
        ... # package source code
```

## dependencies.zip

The requirements libraries are built offline manually and stored
in the source S3 bucket location.

