"""Diagnostic only: try EquFlashV2's unfused cuEq Torch CPU path.

The pinned GGNN source's fusion wrapper reads two CUDA-kernel bookkeeping
fields that the CPU SegmentedPolynomialNaive module lacks. Those fields are
not used in the wrapper's forward calculation. This probe skips only their
assignment, keeping the model weights and actual cuEq forward call unchanged.
The source also omits generate_graph_nvalchemi when optional CUDA-only
nvalchemiops is absent. For CPU diagnosis we map it to the source's own
periodic generate_graph path. Numerical validity still requires preflight.
"""

from __future__ import annotations

import os
import sys
import types


def install_cpu_compatibility(source_root: str) -> None:
    sys.path.insert(0, source_root)
    from GGNN.model.EquFlashV2.nn import convolution

    def unfused_cpu_wrapper(conv_tp):
        conv_tp.original_forward = conv_tp.forward

        def forward(self, node_feats, edge_attrs, tp_weights, edge_index):
            sender, receiver = edge_index[1], edge_index[0]
            return self.original_forward(
                [tp_weights, node_feats, edge_attrs],
                {1: sender}, {0: node_feats}, {0: receiver},
            )[0]

        conv_tp.forward = types.MethodType(forward, conv_tp)
        return conv_tp

    convolution.with_cueq_conv_fusion = unfused_cpu_wrapper
    from GGNN.common import utils as graph_utils

    if not hasattr(graph_utils, "generate_graph_nvalchemi"):
        def periodic_cpu_graph(data, cutoff=None):
            edge_index, _distance, distance_vec, cell_offsets, _offsets, neighbors = (
                graph_utils.generate_graph(
                    data, cutoff=cutoff, max_neighbors=500,
                    use_pbc=True, otf_graph=True,
                )
            )
            return edge_index, distance_vec, cell_offsets, neighbors

        graph_utils.generate_graph_nvalchemi = periodic_cpu_graph


def main():
    install_cpu_compatibility(os.environ["EQUFLASH_SOURCE_ROOT"])
    from mlff_modepair_workflow.advanced_stage1 import main as advanced_main
    advanced_main()


if __name__ == "__main__":
    main()
