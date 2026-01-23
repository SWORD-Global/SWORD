# -*- coding: utf-8 -*-
"""
MLX Tiramisu FCN for River/Bar Semantic Segmentation
====================================================

FC-DenseNet (Tiramisu) implementation in MLX for M-series Mac native execution.
Based on Carbonneau & Bizzi's river segmentation model.

Semantic Classes:
    0: Background
    1: River
    2: Lake
    3: Bar (exposed sediment)

Reference:
    Carbonneau, P. E., & Bizzi, S. (2023). Automatic detection and mapping of
    river bars and bed bathymetry using DenseNet deep learning networks.
    https://doi.org/10.5281/zenodo.7590626
"""

from __future__ import annotations

import logging
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

import numpy as np
from affine import Affine
from rasterio.crs import CRS

try:
    import mlx.core as mx
    import mlx.nn as nn
    MLX_AVAILABLE = True
except ImportError:
    MLX_AVAILABLE = False
    mx = None
    nn = None

logger = logging.getLogger(__name__)

# Semantic class labels
BACKGROUND = 0
RIVER = 1
LAKE = 2
BAR = 3

CLASS_NAMES = {
    BACKGROUND: "background",
    RIVER: "river",
    LAKE: "lake",
    BAR: "bar",
}

# Default weights URL (Zenodo)
WEIGHTS_URL = "https://zenodo.org/record/7590626/files/weights.npz?download=1"
WEIGHTS_DIR = Path(__file__).parent / "weights"
WEIGHTS_PATH = WEIGHTS_DIR / "tiramisu.npz"


def _ensure_mlx():
    """Raise error if MLX is not available."""
    if not MLX_AVAILABLE:
        raise ImportError(
            "MLX is required for TiramisuMLX. Install with: pip install mlx"
        )


# =============================================================================
# FC-DenseNet Building Blocks
# =============================================================================

class DenseLayer(nn.Module):
    """
    Single dense layer: BN → ReLU → Conv3x3(growth).

    Each dense layer outputs `growth` new feature channels that get
    concatenated with the input in the DenseBlock.
    """

    def __init__(self, in_channels: int, growth: int, dropout_rate: float = 0.2):
        super().__init__()
        _ensure_mlx()

        self.bn = nn.BatchNorm(in_channels)
        self.conv = nn.Conv2d(
            in_channels=in_channels,
            out_channels=growth,
            kernel_size=3,
            stride=1,
            padding=1,
        )
        self.dropout_rate = dropout_rate

    def __call__(self, x: mx.array, train: bool = False) -> mx.array:
        out = self.bn(x)
        out = nn.relu(out)
        out = self.conv(out)
        if train and self.dropout_rate > 0:
            out = nn.Dropout(self.dropout_rate)(out)
        return out


class DenseBlock(nn.Module):
    """
    Dense block: stack of DenseLayers with dense connectivity.

    Each layer receives all preceding feature maps as input.
    Output is concatenation of all layer outputs.
    """

    def __init__(
        self,
        in_channels: int,
        n_layers: int,
        growth: int,
        dropout_rate: float = 0.2,
    ):
        super().__init__()
        _ensure_mlx()

        self.layers = []
        for i in range(n_layers):
            layer_in = in_channels + i * growth
            self.layers.append(DenseLayer(layer_in, growth, dropout_rate))

        # Total output channels = all new features from this block
        self.out_channels = n_layers * growth

    def __call__(self, x: mx.array, train: bool = False) -> mx.array:
        features = [x]
        for layer in self.layers:
            # Concatenate all previous features
            concat_in = mx.concatenate(features, axis=-1)  # NHWC format
            new_features = layer(concat_in, train=train)
            features.append(new_features)

        # Return only new features (not input), per Tiramisu design
        return mx.concatenate(features[1:], axis=-1)


class TransitionDown(nn.Module):
    """
    Transition down: BN → ReLU → Conv1x1 → MaxPool2x2.

    Reduces spatial dimensions by 2x while preserving channel count.
    """

    def __init__(self, in_channels: int, dropout_rate: float = 0.2):
        super().__init__()
        _ensure_mlx()

        self.bn = nn.BatchNorm(in_channels)
        self.conv = nn.Conv2d(
            in_channels=in_channels,
            out_channels=in_channels,  # Same channels
            kernel_size=1,
            stride=1,
            padding=0,
        )
        self.dropout_rate = dropout_rate

    def __call__(self, x: mx.array, train: bool = False) -> mx.array:
        out = self.bn(x)
        out = nn.relu(out)
        out = self.conv(out)
        if train and self.dropout_rate > 0:
            out = nn.Dropout(self.dropout_rate)(out)
        # MaxPool2x2
        out = nn.MaxPool2d(kernel_size=2, stride=2)(out)
        return out


class TransitionUp(nn.Module):
    """
    Transition up: Upsample + Conv1x1 for 2x upsampling.

    Uses bilinear upsampling followed by 1x1 conv instead of ConvTranspose2d
    to ensure exact spatial dimension matching with skip connections.
    """

    def __init__(self, in_channels: int, out_channels: int):
        super().__init__()
        _ensure_mlx()

        self.conv = nn.Conv2d(
            in_channels=in_channels,
            out_channels=out_channels,
            kernel_size=1,
            stride=1,
            padding=0,
        )

    def __call__(self, x: mx.array, target_size: Tuple[int, int] = None) -> mx.array:
        """
        Upsample to target size.

        Args:
            x: Input tensor (N, H, W, C)
            target_size: Target (H, W) to upsample to. If None, doubles dimensions.

        Returns:
            Upsampled tensor
        """
        if target_size is None:
            target_size = (x.shape[1] * 2, x.shape[2] * 2)

        # MLX uses NHWC format, resize needs explicit handling
        # Use nearest neighbor upsampling for simplicity (bilinear not in mlx.nn)
        n, h, w, c = x.shape
        target_h, target_w = target_size

        # Simple nearest neighbor upsampling
        # Compute scale factors
        scale_h = target_h / h
        scale_w = target_w / w

        # Create index arrays for nearest neighbor sampling
        y_idx = (mx.arange(target_h) / scale_h).astype(mx.int32)
        x_idx = (mx.arange(target_w) / scale_w).astype(mx.int32)

        # Clamp to valid range
        y_idx = mx.clip(y_idx, 0, h - 1)
        x_idx = mx.clip(x_idx, 0, w - 1)

        # Gather using fancy indexing
        # x has shape (N, H, W, C)
        # We need to upsample H and W dimensions
        upsampled = x[:, y_idx, :, :][:, :, x_idx, :]

        # Apply 1x1 conv
        out = self.conv(upsampled)
        return out


# =============================================================================
# Tiramisu (FC-DenseNet) Main Model
# =============================================================================

class TiramisuMLX(nn.Module):
    """
    FC-DenseNet (Tiramisu) for semantic segmentation.

    Architecture:
        - Initial convolution
        - Encoder: DenseBlock + TransitionDown (repeated)
        - Bottleneck DenseBlock
        - Decoder: TransitionUp + Concat(skip) + DenseBlock (repeated)
        - Final 1x1 conv to n_classes

    Args:
        in_channels: Number of input channels (4 for NIR+R+G+CloudMask)
        n_classes: Number of output classes (4: background, river, lake, bar)
        growth: Growth rate (new channels per dense layer)
        n_layers_down: List of layer counts for encoder dense blocks
        n_layers_up: List of layer counts for decoder dense blocks
        bottleneck_layers: Number of layers in bottleneck
        first_conv_channels: Output channels of initial convolution
        dropout_rate: Dropout rate for dense layers

    Default configuration matches FC-DenseNet56 structure.
    """

    def __init__(
        self,
        in_channels: int = 4,
        n_classes: int = 4,
        growth: int = 16,
        n_layers_down: List[int] = None,
        n_layers_up: List[int] = None,
        bottleneck_layers: int = 5,
        first_conv_channels: int = 48,
        dropout_rate: float = 0.2,
    ):
        super().__init__()
        _ensure_mlx()

        if n_layers_down is None:
            n_layers_down = [4, 5, 7, 10, 12]  # FC-DenseNet103 style
        if n_layers_up is None:
            n_layers_up = list(reversed(n_layers_down))  # Mirror

        self.n_blocks = len(n_layers_down)
        self.growth = growth

        # Initial convolution
        self.first_conv = nn.Conv2d(
            in_channels=in_channels,
            out_channels=first_conv_channels,
            kernel_size=3,
            stride=1,
            padding=1,
        )

        # Encoder path
        self.down_dense_blocks = []
        self.trans_down = []
        skip_channels = []  # Track channels at each skip connection

        channels = first_conv_channels
        for i, n_layers in enumerate(n_layers_down):
            block = DenseBlock(channels, n_layers, growth, dropout_rate)
            self.down_dense_blocks.append(block)

            # After dense block: input channels + new features
            channels = channels + block.out_channels
            skip_channels.append(channels)

            # Transition down (except after last block)
            trans = TransitionDown(channels, dropout_rate)
            self.trans_down.append(trans)

        # Bottleneck
        self.bottleneck = DenseBlock(channels, bottleneck_layers, growth, dropout_rate)
        channels = self.bottleneck.out_channels  # Only new features

        # Decoder path
        self.up_dense_blocks = []
        self.trans_up = []

        for i, n_layers in enumerate(n_layers_up):
            # Transition up
            skip_ch = skip_channels[-(i + 1)]
            trans = TransitionUp(channels, channels)
            self.trans_up.append(trans)

            # After concat with skip
            concat_channels = channels + skip_ch

            block = DenseBlock(concat_channels, n_layers, growth, dropout_rate)
            self.up_dense_blocks.append(block)
            channels = block.out_channels  # Only new features for next trans_up

        # Final convolution
        self.final_conv = nn.Conv2d(
            in_channels=channels,
            out_channels=n_classes,
            kernel_size=1,
            stride=1,
            padding=0,
        )

    def __call__(self, x: mx.array, train: bool = False) -> mx.array:
        """
        Forward pass.

        Args:
            x: Input tensor (N, H, W, C) in NHWC format
            train: Whether in training mode (for dropout)

        Returns:
            Logits tensor (N, H, W, n_classes)
        """
        # Initial conv
        out = self.first_conv(x)

        # Encoder with skip connections
        skips = []
        for i in range(self.n_blocks):
            # Dense block
            new_features = self.down_dense_blocks[i](out, train=train)
            out = mx.concatenate([out, new_features], axis=-1)
            skips.append(out)

            # Transition down
            out = self.trans_down[i](out, train=train)

        # Bottleneck
        out = self.bottleneck(out, train=train)

        # Decoder with skip connections
        for i in range(self.n_blocks):
            # Get skip connection
            skip = skips[-(i + 1)]

            # Transition up - upsample to match skip size
            target_size = (skip.shape[1], skip.shape[2])
            out = self.trans_up[i](out, target_size=target_size)

            # Concatenate skip connection
            out = mx.concatenate([out, skip], axis=-1)

            # Dense block
            out = self.up_dense_blocks[i](out, train=train)

        # Final classification
        logits = self.final_conv(out)

        return logits

    def predict(self, x: mx.array) -> mx.array:
        """Get class predictions (argmax of logits)."""
        logits = self(x, train=False)
        return mx.argmax(logits, axis=-1)

    def predict_proba(self, x: mx.array) -> mx.array:
        """Get class probabilities (softmax of logits)."""
        logits = self(x, train=False)
        return mx.softmax(logits, axis=-1)


# =============================================================================
# Weight Loading
# =============================================================================

def download_weights(url: str = WEIGHTS_URL, path: Path = WEIGHTS_PATH) -> Path:
    """
    Download pre-trained weights from Zenodo.

    Args:
        url: URL to download from
        path: Local path to save weights

    Returns:
        Path to downloaded weights
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    if path.exists():
        logger.info(f"Weights already exist at {path}")
        return path

    logger.info(f"Downloading weights from {url}...")
    urllib.request.urlretrieve(url, path)
    logger.info(f"Weights saved to {path}")

    return path


def load_weights(model: TiramisuMLX, path: Path = WEIGHTS_PATH) -> TiramisuMLX:
    """
    Load pre-trained weights into model.

    Args:
        model: TiramisuMLX model instance
        path: Path to .npz weights file

    Returns:
        Model with loaded weights
    """
    _ensure_mlx()

    path = Path(path)
    if not path.exists():
        logger.info("Weights not found, downloading...")
        download_weights(path=path)

    # Load weights
    weights = dict(np.load(path, allow_pickle=True))

    # Convert numpy arrays to MLX arrays
    mlx_weights = {}
    for key, value in weights.items():
        if isinstance(value, np.ndarray):
            mlx_weights[key] = mx.array(value)
        else:
            mlx_weights[key] = value

    # Load into model
    model.load_weights(list(mlx_weights.items()))

    logger.info(f"Loaded weights from {path}")
    return model


def convert_tf_weights(tf_checkpoint_path: str, output_path: Path = WEIGHTS_PATH) -> Path:
    """
    Convert TensorFlow checkpoint to MLX-compatible .npz format.

    This is a one-time conversion utility. The Carbonneau & Bizzi model
    was trained in TensorFlow, so this maps TF variable names to MLX.

    Args:
        tf_checkpoint_path: Path to TF checkpoint directory
        output_path: Output .npz path

    Returns:
        Path to converted weights
    """
    try:
        import tensorflow as tf
    except ImportError:
        raise ImportError("TensorFlow required for weight conversion")

    # Load TF checkpoint
    reader = tf.train.load_checkpoint(tf_checkpoint_path)
    var_to_shape = reader.get_variable_to_shape_map()

    weights = {}
    for var_name in var_to_shape:
        # Map TF names to MLX names
        # TF uses NHWC, MLX also uses NHWC, so minimal conversion needed
        tensor = reader.get_tensor(var_name)

        # Convert variable name to MLX format
        mlx_name = var_name.replace("/", ".")
        weights[mlx_name] = tensor

    # Save as npz
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez(output_path, **weights)

    logger.info(f"Converted {len(weights)} variables to {output_path}")
    return output_path


# =============================================================================
# Inference Wrapper
# =============================================================================

@dataclass
class TiramisuResult:
    """Result from Tiramisu semantic segmentation."""

    semantic_mask: np.ndarray  # (H, W) class labels
    probabilities: np.ndarray  # (H, W, 4) class probabilities
    river_mask: np.ndarray  # Binary river mask
    bar_mask: np.ndarray  # Binary bar/sediment mask
    lake_mask: np.ndarray  # Binary lake mask
    transform: Affine
    crs: CRS
    stats: Dict[str, Any] = field(default_factory=dict)


class TiramisuSegmenter:
    """
    High-level API for Tiramisu semantic segmentation.

    Handles:
        - Model loading
        - Tiling large images into 224x224 patches
        - Batch inference
        - Stitching results back together
        - Extraction of specific masks (water, bar, etc.)

    Usage:
        segmenter = TiramisuSegmenter()
        result = segmenter.segment(nir, red, green, cloud_mask, transform, crs)
    """

    TILE_SIZE = 224

    def __init__(
        self,
        weights_path: Optional[Path] = None,
        batch_size: int = 8,
        overlap: int = 32,
    ):
        """
        Initialize segmenter.

        Args:
            weights_path: Path to weights file (downloads if missing)
            batch_size: Batch size for inference
            overlap: Overlap between tiles for smoother stitching
        """
        _ensure_mlx()

        self.batch_size = batch_size
        self.overlap = overlap
        self.weights_path = weights_path or WEIGHTS_PATH

        # Initialize model
        self.model = TiramisuMLX(
            in_channels=4,
            n_classes=4,
            growth=16,
            n_layers_down=[4, 5, 7, 10, 12],
            n_layers_up=[12, 10, 7, 5, 4],
            bottleneck_layers=15,
            first_conv_channels=48,
            dropout_rate=0.0,  # No dropout at inference
        )

        # Load weights
        self._load_weights()

    def _load_weights(self):
        """Load pre-trained weights."""
        if not self.weights_path.exists():
            logger.warning(
                f"Weights not found at {self.weights_path}. "
                "Model will use random initialization."
            )
            return

        load_weights(self.model, self.weights_path)

    def _preprocess(
        self,
        nir: np.ndarray,
        red: np.ndarray,
        green: np.ndarray,
        cloud_mask: np.ndarray,
    ) -> np.ndarray:
        """
        Preprocess bands into model input format.

        The Carbonneau model expects NIR, R, G, CloudMask as 4-channel input.
        Values are normalized to [0, 1] range.

        Args:
            nir: NIR band (B08 for S2)
            red: Red band (B04 for S2)
            green: Green band (B03 for S2)
            cloud_mask: Cloud/valid mask (1=clear, 0=cloud)

        Returns:
            Preprocessed 4-channel array (H, W, 4)
        """
        # Stack bands
        stack = np.stack([nir, red, green, cloud_mask], axis=-1).astype(np.float32)

        # Normalize to [0, 1]
        # S2 L2A typically has 0-10000 range for reflectance
        for i in range(3):  # NIR, R, G
            band = stack[..., i]
            valid = band > 0
            if np.any(valid):
                p2, p98 = np.percentile(band[valid], [2, 98])
                band = np.clip(band, p2, p98)
                band = (band - p2) / (p98 - p2 + 1e-8)
                stack[..., i] = band

        # Cloud mask already [0, 1]
        stack[..., 3] = np.clip(cloud_mask, 0, 1)

        return stack

    def _tile_image(
        self,
        image: np.ndarray,
    ) -> Tuple[List[np.ndarray], List[Tuple[int, int]]]:
        """
        Tile image into TILE_SIZE patches with overlap.

        Args:
            image: Input image (H, W, C)

        Returns:
            (list of tiles, list of (row, col) positions)
        """
        h, w = image.shape[:2]
        tile_size = self.TILE_SIZE
        stride = tile_size - self.overlap

        tiles = []
        positions = []

        for row in range(0, h, stride):
            for col in range(0, w, stride):
                # Extract tile
                r_end = min(row + tile_size, h)
                c_end = min(col + tile_size, w)

                tile = image[row:r_end, col:c_end]

                # Pad if needed
                if tile.shape[0] < tile_size or tile.shape[1] < tile_size:
                    padded = np.zeros((tile_size, tile_size, image.shape[2]), dtype=image.dtype)
                    padded[:tile.shape[0], :tile.shape[1]] = tile
                    tile = padded

                tiles.append(tile)
                positions.append((row, col))

        return tiles, positions

    def _stitch_predictions(
        self,
        predictions: List[np.ndarray],
        positions: List[Tuple[int, int]],
        output_shape: Tuple[int, int],
    ) -> np.ndarray:
        """
        Stitch tiled predictions back together.

        Uses weighted averaging in overlap regions.

        Args:
            predictions: List of prediction arrays (TILE_SIZE, TILE_SIZE, n_classes)
            positions: List of (row, col) positions
            output_shape: Original image (H, W)

        Returns:
            Stitched predictions (H, W, n_classes)
        """
        h, w = output_shape
        n_classes = predictions[0].shape[-1]
        tile_size = self.TILE_SIZE

        # Accumulator and weight maps
        output = np.zeros((h, w, n_classes), dtype=np.float32)
        weights = np.zeros((h, w), dtype=np.float32)

        # Create weight mask for blending (higher in center, lower at edges)
        y, x = np.ogrid[:tile_size, :tile_size]
        center = tile_size / 2
        dist_from_edge = np.minimum(
            np.minimum(y, tile_size - 1 - y),
            np.minimum(x, tile_size - 1 - x),
        )
        weight_mask = (dist_from_edge / center).clip(0.1, 1.0)

        for pred, (row, col) in zip(predictions, positions):
            r_end = min(row + tile_size, h)
            c_end = min(col + tile_size, w)
            pred_h = r_end - row
            pred_w = c_end - col

            output[row:r_end, col:c_end] += pred[:pred_h, :pred_w] * weight_mask[:pred_h, :pred_w, np.newaxis]
            weights[row:r_end, col:c_end] += weight_mask[:pred_h, :pred_w]

        # Normalize by weights
        weights = np.maximum(weights, 1e-8)
        output /= weights[..., np.newaxis]

        return output

    def segment(
        self,
        nir: np.ndarray,
        red: np.ndarray,
        green: np.ndarray,
        cloud_mask: np.ndarray,
        transform: Affine,
        crs: CRS,
    ) -> TiramisuResult:
        """
        Segment image into semantic classes.

        Args:
            nir: NIR band array
            red: Red band array
            green: Green band array
            cloud_mask: Cloud/valid mask (1=clear, 0=cloud)
            transform: Affine transform
            crs: CRS

        Returns:
            TiramisuResult with semantic mask and per-class masks
        """
        _ensure_mlx()

        # Preprocess
        image = self._preprocess(nir, red, green, cloud_mask)
        h, w = image.shape[:2]

        # Tile
        tiles, positions = self._tile_image(image)
        n_tiles = len(tiles)
        logger.info(f"Processing {n_tiles} tiles ({h}x{w} image)")

        # Batch inference
        all_preds = []
        for i in range(0, n_tiles, self.batch_size):
            batch_tiles = tiles[i:i + self.batch_size]

            # Convert to MLX
            batch = mx.array(np.stack(batch_tiles, axis=0))

            # Predict
            probs = self.model.predict_proba(batch)
            mx.eval(probs)  # Force computation

            # Convert back to numpy
            all_preds.extend([np.array(probs[j]) for j in range(probs.shape[0])])

        # Stitch
        probs = self._stitch_predictions(all_preds, positions, (h, w))

        # Get class predictions
        semantic_mask = np.argmax(probs, axis=-1).astype(np.uint8)

        # Extract individual masks
        river_mask = (semantic_mask == RIVER).astype(np.uint8)
        lake_mask = (semantic_mask == LAKE).astype(np.uint8)
        bar_mask = (semantic_mask == BAR).astype(np.uint8)

        # Compute statistics
        total_valid = np.sum(cloud_mask > 0.5)
        stats = {
            "total_pixels": int(h * w),
            "valid_pixels": int(total_valid),
            "n_tiles": n_tiles,
        }

        for cls_id, cls_name in CLASS_NAMES.items():
            count = np.sum(semantic_mask == cls_id)
            stats[f"{cls_name}_pixels"] = int(count)
            stats[f"{cls_name}_fraction"] = float(count / max(total_valid, 1))

        logger.info(
            f"Segmentation complete: river={stats['river_fraction']:.2%}, "
            f"bar={stats['bar_fraction']:.2%}, lake={stats['lake_fraction']:.2%}"
        )

        return TiramisuResult(
            semantic_mask=semantic_mask,
            probabilities=probs,
            river_mask=river_mask,
            bar_mask=bar_mask,
            lake_mask=lake_mask,
            transform=transform,
            crs=crs,
            stats=stats,
        )

    def get_water_mask(
        self,
        nir: np.ndarray,
        red: np.ndarray,
        green: np.ndarray,
        cloud_mask: np.ndarray,
        transform: Affine,
        crs: CRS,
        include_lakes: bool = False,
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """
        Get binary water mask (drop-in replacement for NDWI-based extraction).

        Args:
            nir, red, green: Band arrays
            cloud_mask: Cloud/valid mask
            transform: Affine transform
            crs: CRS
            include_lakes: Whether to include lakes in water mask

        Returns:
            (water_mask, stats dict)
        """
        result = self.segment(nir, red, green, cloud_mask, transform, crs)

        # Water = river (optionally + lake)
        if include_lakes:
            water_mask = (result.river_mask | result.lake_mask).astype(np.uint8)
        else:
            water_mask = result.river_mask

        return water_mask, result.stats

    def get_bar_mask(
        self,
        nir: np.ndarray,
        red: np.ndarray,
        green: np.ndarray,
        cloud_mask: np.ndarray,
        transform: Affine,
        crs: CRS,
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        """
        Get binary bar/sediment mask.

        This is a NEW capability not available with NDWI.

        Args:
            nir, red, green: Band arrays
            cloud_mask: Cloud/valid mask
            transform: Affine transform
            crs: CRS

        Returns:
            (bar_mask, stats dict)
        """
        result = self.segment(nir, red, green, cloud_mask, transform, crs)
        return result.bar_mask, result.stats


# =============================================================================
# Integration with Existing Pipeline
# =============================================================================

def segment_with_tiramisu(
    nir: np.ndarray,
    red: np.ndarray,
    green: np.ndarray,
    cloud_mask: np.ndarray,
    transform: Affine,
    crs: CRS,
    weights_path: Optional[Path] = None,
) -> TiramisuResult:
    """
    Convenience function for one-off segmentation.

    For batch processing, create a TiramisuSegmenter instance
    to avoid reloading weights each time.
    """
    segmenter = TiramisuSegmenter(weights_path=weights_path)
    return segmenter.segment(nir, red, green, cloud_mask, transform, crs)
