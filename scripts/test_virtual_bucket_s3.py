#!/usr/bin/env python3
"""
S3兼容API测试脚本
用于测试虚拟存储桶test-virtual-1的S3兼容功能
"""

import boto3
import os
import sys
from pprint import pprint
import json
import hashlib
import tempfile
import time
from botocore.exceptions import ClientError, NoCredentialsError
from datetime import datetime

class S3VirtualBucketTester:
    def __init__(self, endpoint_url, access_key, secret_key, virtual_bucket_name='test-virtual-1'):
        """
        初始化S3虚拟存储桶测试器
        
        Args:
            endpoint_url: S3服务端点URL
            access_key: 访问密钥
            secret_key: 私密密钥
            virtual_bucket_name: 虚拟存储桶名称
        """
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.virtual_bucket_name = virtual_bucket_name
        
        # 创建S3客户端
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name='auto'
        )
        
        # 创建S3资源
        self.s3_resource = boto3.resource(
            's3',
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name='auto'
        )
        
        print(f"初始化S3客户端 - 端点: {self.endpoint_url}")
        print(f"虚拟存储桶: {self.virtual_bucket_name}")
    
    def test_list_buckets(self):
        """测试列出存储桶"""
        print("\n=== 测试列出存储桶 ===")
        try:
            response = self.s3_client.list_buckets()
            pprint(response)
            buckets = response['Buckets']
            
            print(f"找到 {len(buckets)} 个存储桶:")
            for bucket in buckets:
                print(f"  - {bucket['Name']} (创建时间: {bucket['CreationDate']})")
            
            # 检查是否包含我们的虚拟存储桶
            virtual_buckets = [b['Name'] for b in buckets if b['Name'] == self.virtual_bucket_name]
            if virtual_buckets:
                print(f"✓ 虚拟存储桶 '{self.virtual_bucket_name}' 存在")
                return True
            else:
                print(f"✗ 虚拟存储桶 '{self.virtual_bucket_name}' 不存在")
                return False
                
        except ClientError as e:
            print(f"✗ 列出存储桶失败: {e}")
            return False
    
    def test_head_bucket(self):
        """测试检查存储桶是否存在"""
        print("\n=== 测试检查存储桶是否存在 ===")
        try:
            self.s3_client.head_bucket(Bucket=self.virtual_bucket_name)
            print(f"✓ 虚拟存储桶 '{self.virtual_bucket_name}' 存在")
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                print(f"✗ 虚拟存储桶 '{self.virtual_bucket_name}' 不存在")
            else:
                print(f"✗ 检查存储桶失败: {e}")
            return False
    
    def test_list_objects(self):
        """测试列出存储桶中的对象"""
        print("\n=== 测试列出存储桶中的对象 ===")
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.virtual_bucket_name)
            
            if 'Contents' in response:
                objects = response['Contents']
                print(f"存储桶中有 {len(objects)} 个对象:")
                for obj in objects:
                    print(f"  - {obj['Key']} (大小: {obj['Size']} 字节, 修改时间: {obj['LastModified']})")
            else:
                print("存储桶为空")
            
            print("✓ 列出对象成功")
            return True
            
        except ClientError as e:
            print(f"✗ 列出对象失败: {e}")
            return False
    
    def test_put_object(self, object_key, content):
        """测试上传对象"""
        print(f"\n=== 测试上传对象 '{object_key}' ===")
        try:
            # 创建内存中的文件内容
            from io import BytesIO
            file_content = BytesIO(content.encode('utf-8'))
            
            # 上传对象
            response = self.s3_client.put_object(
                Bucket=self.virtual_bucket_name,
                Key=object_key,
                Body=file_content,
                ContentType='text/plain'
            )
            
            print(f"✓ 上传对象成功")
            print(f"  ETag: {response['ETag']}")
            print(f"  版本ID: {response.get('VersionId', 'N/A')}")
            return True
            
        except ClientError as e:
            print(f"✗ 上传对象失败: {e}")
            return False
    
    def test_get_object(self, object_key):
        """测试获取对象"""
        print(f"\n=== 测试获取对象 '{object_key}' ===")
        try:
            response = self.s3_client.get_object(Bucket=self.virtual_bucket_name, Key=object_key)
            
            content = response['Body'].read().decode('utf-8')
            print(f"✓ 获取对象成功")
            print(f"  内容: {content[:100]}{'...' if len(content) > 100 else ''}")
            print(f"  内容长度: {response['ContentLength']}")
            print(f"  最后修改时间: {response['LastModified']}")
            print(f"  ETag: {response['ETag']}")
            print(f"  内容类型: {response['ContentType']}")
            
            return True, content
            
        except ClientError as e:
            print(f"✗ 获取对象失败: {e}")
            return False, None
    
    def test_head_object(self, object_key):
        """测试获取对象元数据"""
        print(f"\n=== 测试获取对象元数据 '{object_key}' ===")
        try:
            response = self.s3_client.head_object(Bucket=self.virtual_bucket_name, Key=object_key)
            
            print(f"✓ 获取对象元数据成功")
            print(f"  内容长度: {response['ContentLength']}")
            print(f"  最后修改时间: {response['LastModified']}")
            print(f"  ETag: {response['ETag']}")
            print(f"  内容类型: {response['ContentType']}")
            
            return True
            
        except ClientError as e:
            print(f"✗ 获取对象元数据失败: {e}")
            return False
    
    def test_delete_object(self, object_key):
        """测试删除对象"""
        print(f"\n=== 测试删除对象 '{object_key}' ===")
        try:
            self.s3_client.delete_object(Bucket=self.virtual_bucket_name, Key=object_key)
            print(f"✓ 删除对象成功")
            return True
            
        except ClientError as e:
            print(f"✗ 删除对象失败: {e}")
            return False
    
    def test_multipart_upload(self, object_key, content_parts):
        """测试分片上传"""
        print(f"\n=== 测试分片上传 '{object_key}' ===")
        try:
            # 检查分片大小要求（R2要求除最后一个分片外，每个分片至少5MiB）
            min_part_size = 5 * 1024 * 1024  # 5MiB
            for i, part_content in enumerate(content_parts[:-1], 1):  # 除了最后一个分片
                part_size = len(part_content.encode('utf-8'))
                if part_size < min_part_size:
                    print(f"  警告: 分片 {i} 大小 ({part_size} 字节) 小于最小要求 ({min_part_size} 字节)")
                else:
                    print(f"  分片 {i} 大小: {part_size / (1024*1024):.1f} MiB ✓")
            
            # 最后一个分片可以小于5MiB
            if content_parts:
                last_part_size = len(content_parts[-1].encode('utf-8'))
                print(f"  最后分片大小: {last_part_size / (1024*1024):.1f} MiB (允许小于5MiB)")
            
            # 初始化分片上传
            response = self.s3_client.create_multipart_upload(
                Bucket=self.virtual_bucket_name,
                Key=object_key,
                ContentType='text/plain'
            )
            upload_id = response['UploadId']
            print(f"  初始化分片上传成功, UploadId: {upload_id}")
            
            # 上传各个分片
            parts = []
            for i, part_content in enumerate(content_parts, 1):
                part_size = len(part_content.encode('utf-8'))
                print(f"  正在上传分片 {i} ({part_size / (1024*1024):.1f} MiB)...")
                
                part_response = self.s3_client.upload_part(
                    Bucket=self.virtual_bucket_name,
                    Key=object_key,
                    PartNumber=i,
                    UploadId=upload_id,
                    Body=part_content.encode('utf-8')
                )
                parts.append({
                    'PartNumber': i,
                    'ETag': part_response['ETag']
                })
                print(f"  上传分片 {i} 成功, ETag: {part_response['ETag']}")
            
            # 完成分片上传
            complete_response = self.s3_client.complete_multipart_upload(
                Bucket=self.virtual_bucket_name,
                Key=object_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
            total_size = sum(len(part.encode('utf-8')) for part in content_parts)
            print(f"✓ 分片上传成功")
            print(f"  总大小: {total_size / (1024*1024):.1f} MiB")
            print(f"  ETag: {complete_response['ETag']}")
            print(f"  位置: {complete_response['Location']}")
            return True
            
        except ClientError as e:
            print(f"✗ 分片上传失败: {e}")
            return False
    
    def test_object_versions(self, object_key):
        """测试对象版本控制（如果支持）"""
        print(f"\n=== 测试对象版本控制 '{object_key}' ===")
        try:
            response = self.s3_client.list_object_versions(Bucket=self.virtual_bucket_name, Prefix=object_key)
            
            if 'Versions' in response:
                versions = response['Versions']
                print(f"找到 {len(versions)} 个版本:")
                for version in versions:
                    print(f"  - 版本ID: {version['VersionId']}, 大小: {version['Size']}, 最后修改: {version['LastModified']}")
            else:
                print("没有找到版本信息")
            
            print("✓ 版本控制测试成功")
            return True
            
        except ClientError as e:
            print(f"✗ 版本控制测试失败: {e}")
            return False
    
    def test_large_file_operations(self):
        """测试大文件操作"""
        print(f"\n=== 测试大文件操作 ===")
        
        # 创建临时大文件（10MB）
        large_content = 'A' * (10 * 1024 * 1024)  # 10MB
        object_key = f"large_file_{int(time.time())}.txt"
        
        # 测试上传
        if not self.test_put_object(object_key, large_content):
            return False
        
        # 测试获取
        success, content = self.test_get_object(object_key)
        if not success:
            return False
        
        # 验证内容完整性
        if content == large_content:
            print("✓ 大文件内容完整性验证通过")
        else:
            print("✗ 大文件内容完整性验证失败")
            return False
        
        # 测试删除
        if not self.test_delete_object(object_key):
            return False
        
        return True
    
    def run_comprehensive_test(self):
        """运行综合测试"""
        print("开始S3虚拟存储桶综合测试...")
        print(f"时间: {datetime.now().isoformat()}")
        
        test_results = []
        
        # 1. 测试列出存储桶
        test_results.append(("列出存储桶", self.test_list_buckets()))
        
        # 2. 测试检查存储桶是否存在
        test_results.append(("检查存储桶", self.test_head_bucket()))
        
        # 3. 测试初始列出对象
        test_results.append(("初始列出对象", self.test_list_objects()))
        
        # 4. 测试基本对象操作
        test_objects = [
            ("test1.txt", "Hello, World!"),
            ("test2.txt", "这是一个测试文件"),
            ("test3.txt", "Testing virtual bucket functionality"),
            ("folder/test4.txt", "Nested file test"),
            ("folder/subfolder/test5.txt", "Deep nested file test")
        ]
        
        for object_key, content in test_objects:
            # 上传对象
            upload_success = self.test_put_object(object_key, content)
            test_results.append((f"上传 {object_key}", upload_success))
            
            if upload_success:
                # 获取对象
                get_success, retrieved_content = self.test_get_object(object_key)
                test_results.append((f"获取 {object_key}", get_success))
                
                if get_success and retrieved_content == content:
                    print(f"✓ 内容完整性验证通过: {object_key}")
                else:
                    print(f"✗ 内容完整性验证失败: {object_key}")
                
                # 获取对象元数据
                test_results.append((f"获取元数据 {object_key}", self.test_head_object(object_key)))
        
        # 5. 再次列出对象
        test_results.append(("列出所有对象", self.test_list_objects()))
        
        # 6. 测试分片上传
        # R2要求每个分片上传大小不小于5MiB (5,242,880 字节)，除了最后一个分片
        min_part_size = 5 * 1024 * 1024  # 5MiB
        part1_content = 'A' * min_part_size  # 第一个分片 5MiB
        part2_content = 'B' * min_part_size  # 第二个分片 5MiB
        part3_content = 'C' * (1024 * 1024)  # 最后一个分片 1MiB (可以小于5MiB)
        multipart_content = [part1_content, part2_content, part3_content]
        test_results.append(("分片上传", self.test_multipart_upload("multipart_test.txt", multipart_content)))
        
        # 7. 测试对象版本控制
        test_results.append(("版本控制", self.test_object_versions("test1.txt")))
        
        # 8. 测试大文件操作
        test_results.append(("大文件操作", self.test_large_file_operations()))
        
        # 9. 清理测试对象
        cleanup_success = True
        for object_key, _ in test_objects:
            if not self.test_delete_object(object_key):
                cleanup_success = False
        test_results.append(("清理测试对象", cleanup_success))
        
        # 清理分片上传对象
        self.test_delete_object("multipart_test.txt")
        
        # 10. 最终列出对象
        test_results.append(("最终列出对象", self.test_list_objects()))
        
        # 打印测试结果汇总
        print("\n" + "="*60)
        print("测试结果汇总")
        print("="*60)
        
        passed = 0
        failed = 0
        
        for test_name, result in test_results:
            status = "✓ 通过" if result else "✗ 失败"
            print(f"{test_name:25} {status}")
            if result:
                passed += 1
            else:
                failed += 1
        
        print("-"*60)
        print(f"总计: {passed + failed} 个测试")
        print(f"通过: {passed} 个")
        print(f"失败: {failed} 个")
        
        if failed == 0:
            print("\n🎉 所有测试通过！虚拟存储桶功能正常。")
            return True
        else:
            print(f"\n❌ 有 {failed} 个测试失败，请检查配置和服务状态。")
            return False

def main():
    """主函数"""
    # 从环境变量或配置文件中获取连接信息
    endpoint_url = os.getenv('S3_ENDPOINT_URL', 'http://localhost:8081')
    access_key = os.getenv('S3_ACCESS_KEY', 'AKIAIOSFODNN7EXAMPLE')
    secret_key = os.getenv('S3_SECRET_KEY', 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY')
    virtual_bucket_name = os.getenv('S3_VIRTUAL_BUCKET', 'test-virtual-1')
    
    # 创建测试器实例
    tester = S3VirtualBucketTester(
        endpoint_url=endpoint_url,
        access_key=access_key,
        secret_key=secret_key,
        virtual_bucket_name=virtual_bucket_name
    )
    
    # 运行测试
    success = tester.run_comprehensive_test()

    # 根据测试结果返回适当的退出码
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()