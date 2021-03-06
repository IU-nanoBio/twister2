package edu.iu.dsc.tws.dashboard.services;

import java.util.Optional;

import javax.persistence.EntityNotFoundException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import edu.iu.dsc.tws.dashboard.data_models.ComputeResource;
import edu.iu.dsc.tws.dashboard.data_models.Job;
import edu.iu.dsc.tws.dashboard.data_models.composite_ids.ComputeResourceId;
import edu.iu.dsc.tws.dashboard.repositories.ComputeResourceRepository;

@Service
public class ComputeResourceService {

  @Autowired
  private ComputeResourceRepository computeResourceRepository;

  @Autowired
  private JobService jobService;

  public ComputeResource save(String jobId, ComputeResource computeResource) {
    Job jobById = jobService.getJobById(jobId);
    computeResource.setJob(jobById);
    return computeResourceRepository.save(computeResource);
  }

  public ComputeResource save(ComputeResource computeResource) {
    return computeResourceRepository.save(computeResource);
  }

  public ComputeResource findById(ComputeResourceId computeResourceId) {
    Optional<ComputeResource> byId = computeResourceRepository.findById(
        computeResourceId
    );
    if (byId.isPresent()) {
      return byId.get();
    }
    this.throwNoSuchComputeResourceException(
        computeResourceId.getJob(),
        computeResourceId.getIndex()
    );
    return null;
  }

  public ComputeResource findById(String jobId, Integer index) {
    return this.findById(
        this.createComputerResourceId(jobId, index)
    );
  }

  @Transactional
  public void delete(String jobId, Integer index) {
    computeResourceRepository.deleteById(
        this.createComputerResourceId(jobId, index)
    );
  }

  public ComputeResourceId createComputerResourceId(String jobId, Integer index) {
    ComputeResourceId computeResourceId = new ComputeResourceId();
    computeResourceId.setIndex(index);
    computeResourceId.setJob(jobId);
    return computeResourceId;
  }

  public ComputeResource getScalableComputeResourceForJob(String job) {
    return this.computeResourceRepository
        .findDistinctByJob_JobIDAndScalable(job, true);
  }

//  @Transactional
//  public void scale(String jobId, Integer index,
//                    ScaleWorkersRequest scaleWorkersRequest) {
//    int scaledAmount = this.computeResourceRepository.scale(
//        jobId, index, scaleWorkersRequest.getInstances()
//    );
//    if (scaledAmount == 0) {
//      this.throwNoSuchComputeResourceException(jobId, index);
//    }
//  }

  private void throwNoSuchComputeResourceException(String jobId, Integer index) {
    throw new EntityNotFoundException("No such compute resource defined with id"
        + index + " for job " + jobId);
  }
}
