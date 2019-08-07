import pyspark
from pyspark import SparkContext
import cv2
import numpy as np
import scipy as sp
import struct
from helper_functions import *
from constants import *


def flatmap_YCrCb(arg):
    # arg[0] = image_id      arg[1] = img_matrix
    Y, crf, cbf = convert_to_YCrCb(arg[1])
    new_arg = []
    og_height = Y.shape[0]
    og_width = Y.shape[1]

    new_arg.append(((arg[0], "y", og_height, og_width), Y))
    new_arg.append(((arg[0], "cr", og_height, og_width), crf))
    new_arg.append(((arg[0], "cb", og_height, og_width), cbf))
    return new_arg

def map_YCrCb(arg):
    #arg = ((id, string_channel, og_height, og_width), channel_matrix)
    new_image= truncate((None, arg[1]))[1]
    return(arg[0], new_image)


def flatmap_subblocks(arg):

    #arg[0] = (id, string_channel, og_height, og_width)     arg[1] = (2d_matrix for channel)

    height = arg[1].shape[0]
    width = arg[1].shape[1]
    key = arg[0]
    eight_by_eight = []
    no_vert_blocks = width / b_size # divide by 8
    no_horz_blocks = height / b_size # divide by 8
    for j in range(no_vert_blocks):
            for i in range(no_horz_blocks):
                coord = (i, j)
                i_start = i * b_size
                i_end = (i + 1) * b_size
                j_start = j * b_size
                j_end = (j + 1) * b_size
                cur_block = arg[1][i_start : i_end, j_start : j_end].astype(np.float32)
                key_val = (key, (cur_block, coord, height, width))
                eight_by_eight.append(key_val)
    return eight_by_eight


def subblock_map(arg):

    # arg[0] = (id, string_channel, og_height, og_width)     arg[1] = (eight_by_eight, coord, height, width)
    return(arg[0], arg[1])

def transformation_map(arg):
    subblock = arg[1][0]
    subblock = dct_block(subblock.astype(np.float32) - 128)
    subblock = quantize_block(subblock, arg[0][1] == "y", QF)
    subblock = quantize_block(subblock, arg[0][1] == "y", QF, inverse = True)
    subblock = dct_block(subblock, inverse = True)
    subblock = subblock + 128
    subblock[subblock>255] = 255
    subblock[subblock<0] = 0
    return (arg[0], [arg[1]])


def first_reduce(arg1, arg2):
    return arg1+arg2

def combine_map(arg):

    # arg[0] = (id, string_channel, og_height, og_width)     arg[1] = ((eight_by_eight, coord, height, width), (eight_by_eight, coord, height, width)....)
    dst = np.zeros((arg[1][0][2], arg[1][0][3]), np.float32)
    for matrix in arg[1]:
        i_start = matrix[1][0] * b_size
        i_end = (matrix[1][0] + 1) * b_size
        j_start = matrix[1][1] * b_size
        j_end = (matrix[1][1] + 1) * b_size
        dst[i_start : i_end, j_start : j_end] = matrix[0]
    dst = resize_image(dst, arg[0][3], arg[0][2])
    return ((arg[0][0], arg[0][2], arg[0][3]), [(dst, arg[0][1])])

def final_reduce(arg1, arg2):
    return arg1 + arg2

def final_combine(arg):

    reimg = np.zeros((arg[0][1], arg[0][2], 3), np.uint8)
    depth = 0
    for pair in arg[1]:
        if pair[1] == "y":
            depth = 0
            reimg[:,:,depth] = pair[0].astype(np.uint8)
        elif pair[1] == "cr":
            depth = 1
            reimg[:,:,depth] = pair[0].astype(np.uint8)
        else:
            depth = 2
            reimg[:,:,depth] = pair[0].astype(np.uint8)
    reimg = to_rgb(reimg)
    return (arg[0][0], reimg)



### WRITE ALL HELPER FUNCTIONS ABOVE THIS LINE ###

def generate_Y_cb_cr_matrices(rdd):
    """
    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    rdd = rdd.flatMap(flatmap_YCrCb).map(map_YCrCb)
    return rdd

def generate_sub_blocks(rdd):
    """
    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    rdd = rdd.flatMap(flatmap_subblocks).map(subblock_map)
    return rdd

def apply_transformations(rdd):
    """
    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    rdd = rdd.map(transformation_map)
    return rdd

def combine_sub_blocks(rdd):
    """
    Given an rdd of subblocks from many different images, combine them together to reform the images.
    Should your rdd should contain values that are np arrays of size (height, width).

    THIS FUNCTION MUST RETURN AN RDD
    """
    ### BEGIN SOLUTION ###
    rdd = rdd.reduceByKey(first_reduce).map(combine_map)
    return rdd

def finished_product(rdd):
    rdd = rdd.reduceByKey(final_reduce).map(final_combine)
    return rdd

def run(images):
    """
    THIS FUNCTION MUST RETURN AN RDD

    Returns an RDD where all the images will be proccessed once the RDD is aggregated.
    The format returned in the RDD should be (image_id, image_matrix) where image_matrix
    is an np array of size (height, width, 3).
    """
    sc = SparkContext()
    rdd = sc.parallelize(images, 16) \
        .map(truncate).repartition(16)
    rdd = generate_Y_cb_cr_matrices(rdd)
    rdd = generate_sub_blocks(rdd)
    rdd = apply_transformations(rdd)
    rdd = combine_sub_blocks(rdd)


    ### BEGIN SOLUTION HERE ###
    # Add any other necessary functions you would like to perform on the rdd here
    # Feel free to write as many helper functions as necessary

    rdd = finished_product(rdd)
    return  rdd


"""
from spark_image_compressor import *
image = cv2.imread("test/test1.jpg", cv2.IMREAD_UNCHANGED)
image_collection = [(x, image) for x in range(10)]
rdd = sc.parallelize(image_collection, 16).map(truncate).repartition(16)
rdd = generate_Y_cb_cr_matrices(rdd)
rdd = generate_sub_blocks(rdd)
rdd = apply_transformations(rdd)
rdd = combine_sub_blocks(rdd)
"""
