package com.springboot.utils;

import io.minio.*;
import io.minio.http.Method;
import io.minio.messages.Bucket;
import io.minio.messages.DeleteError;
import io.minio.messages.DeleteObject;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.annotation.Resource;
import java.io.InputStream;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author xiaomai
 * @description Minio操作的工具类
 * @date 2023/7/4 10:31
 */
@Component
@Slf4j
public class MinioUtil {

    @Resource
    private MinioClient minioClient;

    /**
     * 上传文件到存储桶
     *
     * @param file       文件
     * @param bucketName 存储桶名称
     * @param rename     是否重命名
     */
    public void uploadFile(MultipartFile file, String bucketName, boolean rename) throws Exception {
        // 获取文件名
        String filename = file.getOriginalFilename();
        // 获取文件扩展名
        String fileExtension = StringUtils.getFilenameExtension(filename);
        // 设置存储对象名称
        String uuid = UUID.randomUUID().toString();
        uuid = uuid.replaceAll("-", "");
        // 获取单天日期
        String currentDate = LocalDate.now().toString();
        String newName = currentDate + "/" + uuid + "." + fileExtension;
        // 文件是否重命名
        String objectName = rename ? newName : currentDate + "/" + filename;

        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .stream(file.getInputStream(), file.getSize(), -1)
                        .contentType(file.getContentType())
                        .build()
        );
    }

    /**
     * 上传文件到存储桶
     *
     * @param file       文件
     * @param bucketName 存储桶名称
     * @param objectName 对象名称（文件在存储桶中的路径）
     */
    public void uploadFile(MultipartFile file, String bucketName, String objectName) throws Exception {
        minioClient.putObject(
                PutObjectArgs.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .stream(file.getInputStream(), file.getSize(), -1)
                        .contentType(file.getContentType())
                        .build()
        );
    }


    /**
     * 下载存储桶中的文件
     *
     * @param bucketName 存储桶名称
     * @param objectName 对象名称（文件在存储桶中的路径）
     * @return 文件输入流
     */
    public InputStream downloadFile(String bucketName, String objectName) throws Exception {
        InputStream stream = minioClient.getObject(
                GetObjectArgs.builder().bucket(bucketName).object(objectName).build()
        );
        return stream;
    }

    /**
     * 获取存储桶中的所有文件列表
     *
     * @param bucketName 存储桶名称
     * @return 文件列表
     */
    public List<String> listObjects(String bucketName) throws Exception {
        ArrayList<String> names = new ArrayList<>();

        Iterable<Result<Item>> objects = minioClient.listObjects(
                ListObjectsArgs.builder().bucket(bucketName).recursive(true).build()
        );
        for (Result<Item> result : objects) {
            names.add(result.get().objectName());
        }

        return names;
    }

    /**
     * 获取文件信息
     *
     * @param bucketName 存储桶名称
     * @param objectName 对象名称（文件在存储桶中的路径）
     * @return 文件信息
     */
    public String getObject(String bucketName, String objectName) throws Exception {
        return minioClient.statObject(StatObjectArgs.builder().bucket(bucketName).object(objectName).build()).toString();
    }

    /**
     * 预览文件
     *
     * @param bucketName 存储桶名称
     * @param objectName 对象名称（文件在存储桶中的路径）
     * @param expires    有效期时间
     * @return 预览链接
     */
    public String getPresignedObjectUrl(String bucketName, String objectName, Integer expires) throws Exception {
        return minioClient.getPresignedObjectUrl(
                GetPresignedObjectUrlArgs.builder()
                        .bucket(bucketName).object(objectName).expiry(expires).method(Method.GET).build()
        );
    }


    /**
     * 删除存储桶中的文件
     *
     * @param bucketName 存储桶名称
     * @param objectName 对象名称（文件在存储桶中的路径）
     */
    public void deleteObject(String bucketName, String objectName) throws Exception {
        minioClient.removeObject(RemoveObjectArgs.builder().bucket(bucketName).object(objectName).build());
    }

    /**
     * 删除多个存储桶中的文件
     *
     * @param bucketName  存储桶名称
     * @param objectNames 对象名称列表（文件在存储桶中的路径）
     */
    public void deleteObjects(String bucketName, List<String> objectNames) throws Exception {
        // 构建 DeleteObject 列表
        List<DeleteObject> deleteObjects = objectNames.stream()
                .map(objectName -> new DeleteObject(objectName))
                .collect(Collectors.toList());
        Iterable<Result<DeleteError>> results =
                minioClient.removeObjects(RemoveObjectsArgs.builder().bucket(bucketName).objects(deleteObjects).build());
        for (Result<DeleteError> result : results) {
            DeleteError error = result.get();
            log.error("Object deletion error: {}", error.message());
        }
    }

    /**
     * 同步合并文件
     *
     * @param objectNames 需要合并的资源文件
     * @param oldBucket   源文件存储桶名称
     * @param newBucket   新文件存储桶名称
     * @param fileName    合并后文件名称
     * @return
     */
    public String composeObject(List<String> objectNames, String oldBucket, String newBucket, String fileName) throws Exception {
        // 构建 ComposeSource 列表
        List<ComposeSource> composeSourceList = objectNames.stream()
                .map(objectName -> ComposeSource.builder()
                        .bucket(oldBucket)
                        .object(objectName)
                        .build())
                .collect(Collectors.toList());
        minioClient.composeObject(
                ComposeObjectArgs.builder().bucket(newBucket).object(fileName).sources(composeSourceList).build());
        return fileName;
    }

    /**
     * 同步合并文件
     *
     * @param objectNames 需要合并的资源文件
     * @param bucketName  存储桶名称
     * @param fileName    合并后文件名称
     * @return
     */
    public String composeObject(List<String> objectNames, String bucketName, String fileName) throws Exception {
        // 构建 ComposeSource 列表
        List<ComposeSource> composeSourceList = objectNames.stream()
                .map(objectName -> ComposeSource.builder()
                        .bucket(bucketName)
                        .object(objectName)
                        .build())
                .collect(Collectors.toList());
        minioClient.composeObject(
                ComposeObjectArgs.builder().bucket(bucketName).object(fileName).sources(composeSourceList).build());
        return fileName;
    }


    /**
     * 创建存储桶
     *
     * @param bucketName 存储桶名称
     */
    public void createBucket(String bucketName) throws Exception {
        if (!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucketName).build())) {
            minioClient.makeBucket(MakeBucketArgs.builder().bucket(bucketName).build());
        }
    }

    /**
     * 获取所有存储桶列表
     *
     * @return 存储桶列表
     */
    public List<String> listBuckets() throws Exception {
        List<String> names = new ArrayList<>();

        List<Bucket> list = minioClient.listBuckets();
        list.forEach(bucket -> {
            names.add(bucket.name());
        });

        return names;
    }

    /**
     * 删除存储桶
     *
     * @param bucketName 存储桶名称
     */
    public void deleteBucket(String bucketName) throws Exception {
        minioClient.removeBucket(RemoveBucketArgs.builder().bucket(bucketName).build());
    }


    /**
     * 大文件分片上传
     *
     * @param file        大文件
     * @param fileName    对象名称（文件在存储桶中的路径）
     * @param sliceIndex  分片索引
     * @param totalPieces 切片总数
     * @param md5         整体文件MD5
     * @return
     */
    public String uploadBigFile(MultipartFile file, String bucketName, String fileName, int sliceIndex, int totalPieces, String md5) throws Exception {
        int index = this.uploadBigFileCore(file, sliceIndex, totalPieces, md5);
        if (index == -1) {
            // 完成上传从缓存目录合并迁移到正式目录
            this.createBucket(bucketName);
            // 缓存的所有文件目录
            List<String> objectNames = Stream.iterate(0, i -> ++i)
                    .limit(totalPieces)
                    .map(i -> md5.concat("/").concat(String.valueOf(i)))
                    .collect(Collectors.toList());

            // 合并分片文件
            this.composeObject(objectNames, "temp", bucketName, fileName);

            // 删除所有的分片文件
            this.deleteObjects("temp", objectNames);

            // 验证md5
            InputStream stream = minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(fileName)
                    .build());
            String md5Hex = DigestUtils.md5DigestAsHex(stream);
            if (!md5Hex.equals(md5)) {
                return "-2";
            }

        }
        log.info("文件分片上传，md5:{}, 索引:{}, 总数：{}, 返回：{}", md5, sliceIndex, totalPieces, index);
        return index + "";

    }

    /**
     * 大文件分片上传核心
     *
     * @param file        大文件
     * @param sliceIndex  分片索引
     * @param totalPieces 切片总数
     * @param md5         整体文件MD5
     * @return
     */
    public int uploadBigFileCore(MultipartFile file, int sliceIndex, int totalPieces, String md5) throws Exception {
        // 存放目录
        this.createBucket("temp");
        // 查出已上传的文件
        Iterable<Result<Item>> results = minioClient.listObjects(
                ListObjectsArgs.builder().bucket("temp").prefix(md5.concat("/")).build());
        Set<String> objectNames = new HashSet();
        for (Result<Item> item : results) {
            objectNames.add(item.get().objectName());
        }
        // 过滤已上传的文件
        List<Integer> indexs = Stream.iterate(0, i -> ++i)
                .limit(totalPieces)
                .filter(i -> !objectNames.contains(md5.concat("/").concat(Integer.toString(i))))
                .sorted()
                .collect(Collectors.toList());
        // 返回需要上传的文件序号，-1是上传完成
        if (indexs.size() > 0) {
            if (!indexs.get(0).equals(sliceIndex)) {
                return indexs.get(0);
            }
        } else {
            return -1;
        }
        // 写入文件
        this.uploadFile(file, "temp", md5.concat("/").concat(Integer.toString(sliceIndex)));
        if (sliceIndex < totalPieces - 1) {
            return ++sliceIndex;
        } else {
            return -1;
        }
    }

}
