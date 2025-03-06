package io.mosip.authentication.common.service.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.validation.constraints.NotNull;
import java.time.LocalDateTime;

/**
 * The entity for the Identity records stored in identity_cache table in IDA DB.
 *
 * @author Loganathan Sekar
 */
@Getter 
@Setter 
@Entity
@NoArgsConstructor
@Table(schema = "ida", name = "ida_tracker")
public class TrackerEntity {

	/** The id. */
	@Id
	@NotNull
	@Column(name = "id")
	private String id;

	@NotNull
	@Column(name = "batch_id")
	private String batchId;

	@NotNull
	@Column(name = "st_dtimes")
	private LocalDateTime stDTimes;

	@NotNull
	@Column(name = "transaction_id")
	private String transactionId;

	@NotNull
	@Column(name = "time_taken")
	private Integer timeTaken;


	@NotNull
	@Column(name = "cr_dtimes")
	private LocalDateTime crDTimes;
	
}
