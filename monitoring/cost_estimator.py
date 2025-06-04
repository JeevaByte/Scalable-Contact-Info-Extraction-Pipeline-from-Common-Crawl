"""
Cost estimator for the contact extraction pipeline.
This module calculates and estimates costs for EMR Serverless jobs.
"""
import boto3
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CostEstimator:
    """Cost estimator for EMR Serverless jobs."""
    
    # EMR Serverless pricing (us-east-1, as of 2023)
    # These are example prices and should be updated with actual prices
    DRIVER_VCPU_PRICE_PER_HOUR = 0.052  # $0.052 per vCPU hour
    DRIVER_MEMORY_PRICE_PER_GB_HOUR = 0.0075  # $0.0075 per GB hour
    EXECUTOR_VCPU_PRICE_PER_HOUR = 0.052  # $0.052 per vCPU hour
    EXECUTOR_MEMORY_PRICE_PER_GB_HOUR = 0.0075  # $0.0075 per GB hour
    
    def __init__(self, region: str = "us-east-1"):
        """Initialize the cost estimator.
        
        Args:
            region: AWS region
        """
        self.region = region
        self.emr_client = boto3.client('emr-serverless', region_name=region)
        self.cloudwatch_client = boto3.client('cloudwatch', region_name=region)
        logger.info(f"CostEstimator initialized for region: {region}")
    
    def get_job_run_details(self, application_id: str, job_run_id: str) -> Dict[str, Any]:
        """Get details of an EMR Serverless job run.
        
        Args:
            application_id: EMR Serverless application ID
            job_run_id: EMR Serverless job run ID
            
        Returns:
            Job run details
        """
        response = self.emr_client.get_job_run(
            applicationId=application_id,
            jobRunId=job_run_id
        )
        
        return response['jobRun']
    
    def get_job_run_metrics(self, application_id: str, job_run_id: str) -> Dict[str, Any]:
        """Get metrics for an EMR Serverless job run.
        
        Args:
            application_id: EMR Serverless application ID
            job_run_id: EMR Serverless job run ID
            
        Returns:
            Job run metrics
        """
        # Get job run details
        job_run = self.get_job_run_details(application_id, job_run_id)
        
        # Calculate duration
        start_time = job_run.get('startTime', datetime.now())
        end_time = job_run.get('endTime', datetime.now())
        
        if isinstance(start_time, str):
            start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
        if isinstance(end_time, str):
            end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
        duration_seconds = (end_time - start_time).total_seconds()
        duration_hours = duration_seconds / 3600
        
        # Get driver and executor configurations
        driver_config = job_run.get('configurationOverrides', {}).get('driverConfiguration', {})
        executor_config = job_run.get('configurationOverrides', {}).get('executorConfiguration', {})
        
        # Extract CPU and memory
        driver_cpu = self._extract_cpu(driver_config.get('cpu', '4 vCPU'))
        driver_memory = self._extract_memory(driver_config.get('memory', '16 GB'))
        executor_cpu = self._extract_cpu(executor_config.get('cpu', '4 vCPU'))
        executor_memory = self._extract_memory(executor_config.get('memory', '16 GB'))
        
        # Get number of executors
        executor_instances = job_run.get('sparkSubmitParameters', {}).get('--conf spark.executor.instances', 10)
        if isinstance(executor_instances, str):
            executor_instances = int(executor_instances)
        
        # Calculate costs
        driver_cpu_cost = self.DRIVER_VCPU_PRICE_PER_HOUR * driver_cpu * duration_hours
        driver_memory_cost = self.DRIVER_MEMORY_PRICE_PER_GB_HOUR * driver_memory * duration_hours
        executor_cpu_cost = self.EXECUTOR_VCPU_PRICE_PER_HOUR * executor_cpu * executor_instances * duration_hours
        executor_memory_cost = self.EXECUTOR_MEMORY_PRICE_PER_GB_HOUR * executor_memory * executor_instances * duration_hours
        
        total_cost = driver_cpu_cost + driver_memory_cost + executor_cpu_cost + executor_memory_cost
        
        return {
            'jobRunId': job_run_id,
            'applicationId': application_id,
            'state': job_run.get('state'),
            'startTime': start_time.isoformat(),
            'endTime': end_time.isoformat(),
            'durationSeconds': duration_seconds,
            'durationHours': duration_hours,
            'driverCpu': driver_cpu,
            'driverMemory': driver_memory,
            'executorCpu': executor_cpu,
            'executorMemory': executor_memory,
            'executorInstances': executor_instances,
            'driverCpuCost': driver_cpu_cost,
            'driverMemoryCost': driver_memory_cost,
            'executorCpuCost': executor_cpu_cost,
            'executorMemoryCost': executor_memory_cost,
            'totalCost': total_cost
        }
    
    def estimate_cost_for_data_size(self, data_size_gb: float, processing_rate_gb_per_hour: float = 10.0) -> Dict[str, Any]:
        """Estimate cost for processing a given data size.
        
        Args:
            data_size_gb: Data size in GB
            processing_rate_gb_per_hour: Processing rate in GB per hour
            
        Returns:
            Cost estimate
        """
        # Estimate duration
        estimated_hours = data_size_gb / processing_rate_gb_per_hour
        
        # Assume default configuration
        driver_cpu = 4
        driver_memory = 16
        executor_cpu = 4
        executor_memory = 16
        executor_instances = 10
        
        # Calculate costs
        driver_cpu_cost = self.DRIVER_VCPU_PRICE_PER_HOUR * driver_cpu * estimated_hours
        driver_memory_cost = self.DRIVER_MEMORY_PRICE_PER_GB_HOUR * driver_memory * estimated_hours
        executor_cpu_cost = self.EXECUTOR_VCPU_PRICE_PER_HOUR * executor_cpu * executor_instances * estimated_hours
        executor_memory_cost = self.EXECUTOR_MEMORY_PRICE_PER_GB_HOUR * executor_memory * executor_instances * estimated_hours
        
        total_cost = driver_cpu_cost + driver_memory_cost + executor_cpu_cost + executor_memory_cost
        
        return {
            'dataSizeGB': data_size_gb,
            'processingRateGBPerHour': processing_rate_gb_per_hour,
            'estimatedHours': estimated_hours,
            'driverCpu': driver_cpu,
            'driverMemory': driver_memory,
            'executorCpu': executor_cpu,
            'executorMemory': executor_memory,
            'executorInstances': executor_instances,
            'driverCpuCost': driver_cpu_cost,
            'driverMemoryCost': driver_memory_cost,
            'executorCpuCost': executor_cpu_cost,
            'executorMemoryCost': executor_memory_cost,
            'totalCost': total_cost
        }
    
    def optimize_resources(self, data_size_gb: float, target_duration_hours: float) -> Dict[str, Any]:
        """Optimize resources for a given data size and target duration.
        
        Args:
            data_size_gb: Data size in GB
            target_duration_hours: Target duration in hours
            
        Returns:
            Optimized resource configuration
        """
        # Calculate required processing rate
        required_rate = data_size_gb / target_duration_hours
        
        # Base processing rate per executor (GB/hour)
        base_rate_per_executor = 1.0
        
        # Calculate required number of executors
        required_executors = int(required_rate / base_rate_per_executor) + 1
        
        # Optimize executor count based on cost efficiency
        if required_executors <= 5:
            executor_cpu = 4
            executor_memory = 16
        elif required_executors <= 20:
            executor_cpu = 4
            executor_memory = 16
            required_executors = min(required_executors, 20)
        else:
            executor_cpu = 8
            executor_memory = 32
            required_executors = min(int(required_executors / 2) + 1, 50)
        
        # Driver configuration
        driver_cpu = 4
        driver_memory = 16
        
        # Calculate costs
        estimated_hours = target_duration_hours
        driver_cpu_cost = self.DRIVER_VCPU_PRICE_PER_HOUR * driver_cpu * estimated_hours
        driver_memory_cost = self.DRIVER_MEMORY_PRICE_PER_GB_HOUR * driver_memory * estimated_hours
        executor_cpu_cost = self.EXECUTOR_VCPU_PRICE_PER_HOUR * executor_cpu * required_executors * estimated_hours
        executor_memory_cost = self.EXECUTOR_MEMORY_PRICE_PER_GB_HOUR * executor_memory * required_executors * estimated_hours
        
        total_cost = driver_cpu_cost + driver_memory_cost + executor_cpu_cost + executor_memory_cost
        
        return {
            'dataSizeGB': data_size_gb,
            'targetDurationHours': target_duration_hours,
            'estimatedHours': estimated_hours,
            'driverCpu': driver_cpu,
            'driverMemory': driver_memory,
            'executorCpu': executor_cpu,
            'executorMemory': executor_memory,
            'executorInstances': required_executors,
            'driverCpuCost': driver_cpu_cost,
            'driverMemoryCost': driver_memory_cost,
            'executorCpuCost': executor_cpu_cost,
            'executorMemoryCost': executor_memory_cost,
            'totalCost': total_cost,
            'sparkSubmitParameters': f"--conf spark.executor.cores={executor_cpu} --conf spark.executor.memory={executor_memory}g --conf spark.driver.cores={driver_cpu} --conf spark.driver.memory={driver_memory}g --conf spark.executor.instances={required_executors}"
        }
    
    @staticmethod
    def _extract_cpu(cpu_str: str) -> int:
        """Extract CPU count from string.
        
        Args:
            cpu_str: CPU string (e.g., "4 vCPU")
            
        Returns:
            CPU count
        """
        if isinstance(cpu_str, int):
            return cpu_str
            
        try:
            return int(cpu_str.split()[0])
        except (ValueError, IndexError):
            return 4  # Default
    
    @staticmethod
    def _extract_memory(memory_str: str) -> int:
        """Extract memory in GB from string.
        
        Args:
            memory_str: Memory string (e.g., "16 GB")
            
        Returns:
            Memory in GB
        """
        if isinstance(memory_str, int):
            return memory_str
            
        try:
            return int(memory_str.split()[0])
        except (ValueError, IndexError):
            return 16  # Default


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Cost estimator for EMR Serverless jobs')
    parser.add_argument('--application-id', type=str, help='EMR Serverless application ID')
    parser.add_argument('--job-run-id', type=str, help='EMR Serverless job run ID')
    parser.add_argument('--data-size', type=float, help='Data size in GB')
    parser.add_argument('--target-duration', type=float, help='Target duration in hours')
    parser.add_argument('--region', type=str, default='us-east-1', help='AWS region')
    parser.add_argument('--output', type=str, help='Output file path')
    
    args = parser.parse_args()
    
    estimator = CostEstimator(region=args.region)
    
    if args.application_id and args.job_run_id:
        # Get metrics for a specific job run
        metrics = estimator.get_job_run_metrics(args.application_id, args.job_run_id)
        print(json.dumps(metrics, indent=2))
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(metrics, f, indent=2)
    
    elif args.data_size:
        if args.target_duration:
            # Optimize resources for a given data size and target duration
            optimization = estimator.optimize_resources(args.data_size, args.target_duration)
            print(json.dumps(optimization, indent=2))
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(optimization, f, indent=2)
        else:
            # Estimate cost for a given data size
            estimate = estimator.estimate_cost_for_data_size(args.data_size)
            print(json.dumps(estimate, indent=2))
            
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(estimate, f, indent=2)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()