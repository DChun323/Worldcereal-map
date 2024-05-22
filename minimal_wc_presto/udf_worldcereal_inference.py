import logging
import urllib.request
import shutil
from pathlib import Path
import sys
import functools
import xarray as xr
from typing import Dict
import numpy as np
from pyproj import Transformer


def _setup_logging():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    return logger

@functools.lru_cache(maxsize=6)
def extract_dependencies(base_url: str, dependency_name: str):

    # Generate absolute path for the dependencies folder
    dependencies_dir = Path.cwd() / 'dependencies'

    # Create the directory if it doesn't exist
    dependencies_dir.mkdir(exist_ok=True, parents=True)


    # Download and extract the model file
    modelfile_url = f"{base_url}/{dependency_name}"
    modelfile, _ = urllib.request.urlretrieve(modelfile_url, filename=dependencies_dir / Path(modelfile_url).name)
    shutil.unpack_archive(modelfile, extract_dir=dependencies_dir)

    # Add the model directory to system path if it's not already there
    abs_path = str(dependencies_dir / Path(modelfile_url).name.split('.zip')[0])

    return(abs_path)


def apply_datacube(cube: xr.DataArray, context:Dict) -> xr.DataArray:
    
    logger = _setup_logging() 
    logger.info("Shape of input: {}".format(cube.shape))

    # shape and indiches for output
    cube = cube.transpose('bands', 't', 'x', 'y')
    cube = cube.fillna(65535)
    orig_dims = list(cube.dims)
    map_dims = cube.shape[2:]

    # Unzip de dependencies on the backend
    logger.info("Unzipping dependencies")
    base_url = "https://s3.waw3-1.cloudferro.com/swift/v1/project_dependencies"
    dependency_name = "wc_presto_onnx_dependencies.zip"
    dep_dir = extract_dependencies(base_url, dependency_name)

    # Append the dependencies
    sys.path.append(str(dep_dir))
    sys.path.append(str(dep_dir) + '/pandas')
    from dependencies.wc_presto_onnx_dependencies.mvp_wc_presto.world_cereal_inference import get_presto_features, classify_with_catboost

    # Run presto inference
    logger.info("Extracting presto features")
    PRESTO_PATH = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/worldcereal-minimal-inference/presto.pt"
    features = get_presto_features(cube, PRESTO_PATH)
    logger.info("Shape of presto output: {}".format(features.shape))

    # run catboost classification
    logger.info("Catboost classification")
    CATBOOST_PATH = "https://artifactory.vgt.vito.be/artifactory/auxdata-public/worldcereal-minimal-inference/wc_catboost.onnx"
    classification = classify_with_catboost(features, CATBOOST_PATH)
    logger.info("Shape of classification output: {}".format(classification.shape))

    # revert to 4D shape for openEO
    logger.info("Revert to 4D xarray") 
    transformer = Transformer.from_crs(f"EPSG:{32631}", "EPSG:4326", always_xy=True)
    longitudes, latitudes = transformer.transform(cube.x, cube.y)

    classification = classification.reshape(map_dims)
    classification = np.expand_dims(np.expand_dims(classification, axis=0), axis=0)
    output = xr.DataArray(classification, dims=orig_dims, coords={'x': longitudes, 'y': latitudes})
    logger.info("Shape of output: {}".format(output.shape))

    return output
















