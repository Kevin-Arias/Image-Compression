# Image Compression With SPARK

Using the SPARK MapReduce programming paradigm (alongside openCV and numpy), I implemented a way to parallelize an image compression algorithm to process multiple images at once that emphasized faster runtime and lower storage space used to store an image. I used Discrete Cosine Transformation (DCT) in my implementation which is a transformation used in JPEG compression that is commonly used to differentiate data points in terms of different frequencies, making it easy to discard higher-frequency components and lower the amount of space used to store an image. The main steps of the algorithm are as follows: load a set of images, convert each image from BGR color space to YCbCr color space, apply DCT and Quantization to an 8 x 8 sub-blocks of each part of each image, apply De-Quantization and Inverse DCT to 8 x 8 sub-blocks of each part of each image, stitch the images back together, and generate a set of proccessed images.

Below are two images, the first one is the original image:

![spark2_qf99](https://user-images.githubusercontent.com/16792195/62656580-5d2c0e00-b919-11e9-977c-3e04d832639f.jpg)

and the second image is one used after image compression:

![test2](https://user-images.githubusercontent.com/16792195/62656587-62895880-b919-11e9-9229-7c4a29962bb3.jpg)
