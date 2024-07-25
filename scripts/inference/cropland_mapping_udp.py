import openeo
from openeo import DataCube
from openeo_gfmap.backend import Backend, BackendContext
from worldcereal.job import WorldCerealProduct, generate_map
from openeo.api.process import Parameter

backend_context = BackendContext(Backend.CDSE_STAGING)

bbox_param = Parameter.bounding_box(name="spatial_extent")
temporal_param = Parameter.temporal_interval(name="temporal_extent")

process_graph: DataCube = generate_map(
        bbox_param,
        temporal_param,
        backend_context=backend_context,
        output_path = "",
        product_type=WorldCerealProduct.CROPTYPE,
        out_format="GTiff",
    )

print(process_graph.flat_graph())

c = openeo.connect(
        "openeo.creo.vito.be"
    ).authenticate_oidc()

def load_markdown(name: str) -> str:
    with open( name, 'r', encoding='utf-8') as file:
        return file.read()

links = [{
    "href":"https://esa-worldcereal.org/",
    "title":"ESA WorldCereal website",
    "rel":"about"
}]


c.save_user_defined_process("worldcereal_croptype",process_graph,public=True,summary="Global cereal detector",parameters=[bbox_param,temporal_param],description=load_markdown("croptype_udp.md"),links=links,categories=["crop types"])