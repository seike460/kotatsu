//! Monthly cost estimation for MicroVM fleets.
//!
//! Unit prices come from the official AWS Lambda pricing page
//! (`aws.amazon.com/lambda/pricing`, Lambda MicroVMs section): compute is
//! billed per-second on baseline + above-baseline consumption, suspend is
//! free of compute charges but snapshots are billed for write (suspend),
//! read (resume/launch) and storage. All rates below are US East
//! (N. Virginia), ARM/Graviton — pass a different [`PriceBook`] for other
//! regions. Data transfer uses standard AWS rates and is out of scope.

use crate::error::Result;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

/// Per-unit prices for a region.
#[derive(Clone, Debug)]
pub struct PriceBook {
    /// $/vCPU-second.
    pub vcpu_per_second: Decimal,
    /// $/GB-second of memory.
    pub mem_gb_per_second: Decimal,
    /// $/GB for snapshot writes (each suspend).
    pub snapshot_write_per_gb: Decimal,
    /// $/GB for snapshot reads (each launch and resume).
    pub snapshot_read_per_gb: Decimal,
    /// $/GB-month for snapshot storage (images + suspended state).
    pub storage_per_gb_month: Decimal,
}

impl PriceBook {
    /// US East (N. Virginia), ARM/Graviton rates.
    ///
    /// Source: `https://aws.amazon.com/lambda/pricing` — "Lambda
    /// MicroVMs - Pricing Examples" uses these exact constants
    /// ($0.0000276944/vCPU-s, $0.0000036667/GB-s, $0.0038/GB write,
    /// $0.00155/GB read, $0.08/GB-month storage).
    pub fn us_east_1() -> Self {
        Self {
            vcpu_per_second: dec!(0.0000276944),
            mem_gb_per_second: dec!(0.0000036667),
            snapshot_write_per_gb: dec!(0.0038),
            snapshot_read_per_gb: dec!(0.00155),
            storage_per_gb_month: dec!(0.08),
        }
    }
}

impl Default for PriceBook {
    fn default() -> Self {
        Self::us_east_1()
    }
}

/// One MicroVM's resource envelope.
///
/// AWS fixes CPU at a 2:1 memory-to-vCPU ratio and scales to 4× at peak:
/// 2 GB / 1 vCPU baseline peaks at 8 GB / 4 vCPU. Only the memory tier is
/// stored — vCPUs and the peak envelope derive from it. The field is
/// private so [`MicrovmSpec::baseline`]'s tier check cannot be bypassed.
#[derive(Clone, Copy, Debug)]
pub struct MicrovmSpec {
    baseline_gb: u32,
}

impl MicrovmSpec {
    /// Builds a spec from a baseline memory tier in whole GB.
    ///
    /// Documented tiers are 0.5/1/2/4/8 GB; the 0.5 GB tier is not
    /// representable here, so the accepted set is 1/2/4/8.
    pub fn baseline(gb: u32) -> Result<Self> {
        if gb == 0 || !gb.is_power_of_two() || gb > 8 {
            return Err(crate::error::Error::invalid(format!(
                "baseline memory must be one of 1/2/4/8 GB (got {gb})"
            )));
        }
        Ok(Self { baseline_gb: gb })
    }

    /// Default tier: 2 GB / 1 vCPU → 8 GB / 4 vCPU peak.
    pub fn default_tier() -> Self {
        Self { baseline_gb: 2 }
    }

    /// Baseline memory (GB).
    pub fn baseline_gb(&self) -> u32 {
        self.baseline_gb
    }

    /// vCPUs at baseline (2:1 memory-to-vCPU; handles the 1 GB → 0.5 vCPU tier).
    pub fn baseline_vcpu_dec(&self) -> Decimal {
        Decimal::from(self.baseline_gb) / dec!(2)
    }

    /// Peak memory (GB) — 4× baseline.
    pub fn peak_gb(&self) -> u32 {
        // Invariant: baseline_gb ∈ {1,2,4,8} by construction, so ≤ 32.
        self.baseline_gb * 4
    }

    /// vCPUs at peak — 4× baseline.
    pub fn peak_vcpu_dec(&self) -> Decimal {
        self.baseline_vcpu_dec() * dec!(4)
    }
}

/// Aggregate usage for one billing period (typically a month).
///
/// `baseline_seconds` and `peak_seconds` **partition** RUNNING time — do
/// not pass total running time as `baseline_seconds` plus the peak subset
/// as `peak_seconds`, or the peak window is billed twice (AWS's own
/// example counts baseline-seconds as the non-peak remainder).
#[derive(Clone, Debug)]
pub struct Usage {
    /// Resource envelope of the MicroVMs.
    pub spec: MicrovmSpec,
    /// Seconds run at baseline resource levels (non-peak RUNNING time).
    pub baseline_seconds: u64,
    /// Seconds run at the peak (4×) resource envelope.
    pub peak_seconds: u64,
    /// `suspend-microvm` events — each bills a snapshot *write* sized as
    /// `spec.baseline_gb`. Count suspends here even if the VM is never
    /// resumed (e.g. `suspended_duration_seconds` expiry).
    pub suspends: u64,
    /// `resume-microvm` events — each bills a snapshot *read*.
    pub resumes: u64,
    /// Fresh launches — each bills a snapshot read sized as `image_gb`.
    pub launches: u64,
    /// GB-hours of suspended state retained (converted to GB-months at
    /// 720 h/month, matching AWS's own example).
    pub suspended_gb_hours: Decimal,
    /// Per-image size in GB; also the snapshot-read size per launch.
    /// `launches > 0` with `image_gb = 0` bills $0 for launch reads.
    pub image_gb: u32,
    /// Number of MicroVM images stored for the whole period.
    ///
    /// Image storage bills a **one-week minimum retention**: an image
    /// kept the full month costs `image_gb` GB-months (this field's
    /// assumption); one churned and deleted within the period still
    /// costs at least `image_gb × 0.25` GB-months — model heavy churn by
    /// folding it into this count accordingly.
    pub image_count: u32,
}

impl Usage {
    /// A usage profile for `spec` with no traffic yet — callers fill in
    /// the fields they measure.
    pub fn new(spec: MicrovmSpec) -> Self {
        Self {
            spec,
            baseline_seconds: 0,
            peak_seconds: 0,
            suspends: 0,
            resumes: 0,
            launches: 0,
            suspended_gb_hours: Decimal::ZERO,
            image_gb: 0,
            image_count: 0,
        }
    }
}

/// Dollars per charge dimension.
#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct CostBreakdown {
    /// Compute: baseline + peak, vCPU + memory.
    pub compute: Decimal,
    /// Snapshot reads (launches + resumes).
    pub snapshot_reads: Decimal,
    /// Snapshot writes (suspends).
    pub snapshot_writes: Decimal,
    /// Snapshot storage (images + suspended state).
    pub snapshot_storage: Decimal,
    /// `compute + reads + writes + storage` (data transfer excluded).
    pub total: Decimal,
}

impl PriceBook {
    /// Estimates the period charges for `usage`.
    ///
    /// Sizing assumptions (AWS-verified where marked):
    /// - Suspend **write**: the suspended snapshot covers memory + disk
    ///   state and is billed at the configured baseline (AWS example).
    /// - Resume **read**: same suspended snapshot → baseline GB.
    /// - Launch **read**: reads the *image* snapshot → `usage.image_gb`.
    ///
    /// All rates and `suspended_gb_hours` must be non-negative.
    pub fn estimate(&self, usage: &Usage) -> Result<CostBreakdown> {
        for (name, v) in [
            ("vcpu_per_second", self.vcpu_per_second),
            ("mem_gb_per_second", self.mem_gb_per_second),
            ("snapshot_write_per_gb", self.snapshot_write_per_gb),
            ("snapshot_read_per_gb", self.snapshot_read_per_gb),
            ("storage_per_gb_month", self.storage_per_gb_month),
            ("suspended_gb_hours", usage.suspended_gb_hours),
        ] {
            if v.is_sign_negative() {
                return Err(crate::error::Error::invalid(format!(
                    "cost estimate: {name} must not be negative"
                )));
            }
        }
        let spec = &usage.spec;
        let gb = Decimal::from(spec.baseline_gb());

        let compute = (Decimal::from(usage.baseline_seconds)
            * (spec.baseline_vcpu_dec() * self.vcpu_per_second + gb * self.mem_gb_per_second))
            + (Decimal::from(usage.peak_seconds)
                * (spec.peak_vcpu_dec() * self.vcpu_per_second
                    + Decimal::from(spec.peak_gb()) * self.mem_gb_per_second));

        let reads = Decimal::from(usage.launches)
            * Decimal::from(usage.image_gb)
            * self.snapshot_read_per_gb
            + Decimal::from(usage.resumes) * gb * self.snapshot_read_per_gb;
        let writes = Decimal::from(usage.suspends) * gb * self.snapshot_write_per_gb;
        let storage = (Decimal::from(usage.image_gb) * Decimal::from(usage.image_count)
            + usage.suspended_gb_hours / dec!(720))
            * self.storage_per_gb_month;

        let total = compute + reads + writes + storage;
        Ok(CostBreakdown {
            compute,
            snapshot_reads: reads,
            snapshot_writes: writes,
            snapshot_storage: storage,
            total,
        })
    }
}
