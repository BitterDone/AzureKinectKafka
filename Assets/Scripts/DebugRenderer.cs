using System;
using System.Collections.Generic;
using UnityEngine;
using Microsoft.Azure.Kinect.Sensor;
using Microsoft.Azure.Kinect.Sensor.BodyTracking;
using Stahle.Utility;
using UnityEngine.UI;
using System.Collections;
using Confluent.Kafka;
using System.Threading.Tasks;

public class DebugRenderer : PersistantSingleton<DebugRenderer>
{
    [HideInInspector]public bool canUpdate = false;
    [HideInInspector]public bool enableKafka = false;
    [SerializeField] public List<Skeleton> skeletons = new List<Skeleton>();
    [SerializeField] GameObject[] blockmanArray;
    public Toggle recordPoseToggle; //dragged in manually 
    public Renderer renderer;
	public GameObject blockPrefab;
	Skeleton skeleton;

    Device device;
    BodyTracker tracker;

	ProducerConfig conf;
	IProducer<string, string> p;

    protected override void Awake()
	{
		Screen.sleepTimeout = SleepTimeout.NeverSleep;
		base.Awake();
		MakeBlockMan();
	}

	public void Start()
	{
		foreach (GameObject go in blockmanArray)
		{
			go.SetActive(true);
		}
		recordPoseToggle.onValueChanged.AddListener(OnToggleValueChanged);

		InitCamera();
		conf = new ProducerConfig{
				BootstrapServers = "localhost:9092",
			};
		//p = new ProducerBuilder<string, string>(conf).Build();
    }

	void producerSendMessage(string message)
	{
		try
		{
			// Note: Awaiting the asynchronous produce request below prevents flow of execution
			// from proceeding until the acknowledgement from the broker is received (at the 
			// expense of low throughput).
			// https://stackoverflow.com/questions/40872520/whats-the-purpose-of-kafkas-key-value-pair-based-messaging
			var deliveryReport = p.ProduceAsync( //await
				"testTopicName", new Message<string, string> { Key = "none", Value = message });

			print($"delivered to: {deliveryReport.Result.TopicPartitionOffset}");
		}
		catch (ProduceException<string, string> e)
		{
			print($"failed to deliver message: {e.Message} [{e.Error.Code}]");
		}
	}

	void MakeBlockMan()
	{
		int numberOfJoints = (int)JointId.Count;

		blockmanArray = new GameObject[numberOfJoints];

		for (var i = 0; i < numberOfJoints; i++)
		{
			GameObject jointCube = Instantiate(blockPrefab, transform);
			//deactivate it - (its Start() or OnEnable() won't be called)
			jointCube.SetActive(false);
			jointCube.name = Enum.GetName(typeof(JointId), i);
			//why do we multiply by .4?  idk
			jointCube.transform.localScale = Vector3.one * 0.4f;
			blockmanArray[i] = jointCube;
		}
	}
	
	void InitCamera()
    {
        this.device = Device.Open(0);
		var config = new DeviceConfiguration
		{
			ColorResolution = ColorResolution.r720p,
			ColorFormat = ImageFormat.ColorBGRA32,
			DepthMode = DepthMode.NFOV_Unbinned
        };
        device.StartCameras(config);

        var calibration = device.GetCalibration(config.DepthMode, config.ColorResolution);
        this.tracker = BodyTracker.Create(calibration);

		GameObject cameraFeed = GameObject.FindWithTag("CameraFeed");
		renderer = cameraFeed.GetComponent<Renderer>();
	}

    void Update()
    {
        if (canUpdate)
        {
            StreamCameraAsTexture();
            CaptureSkeletonsFromCameraFrame();
            //CaptureSkeletonsFromFakeRandomData();
        }
    }
	
	void StreamCameraAsTexture()
	{
		using (Capture capture = device.GetCapture())
		{
			tracker.EnqueueCapture(capture);
			var color = capture.Color;
			if (color.WidthPixels > 0)
			{
				Texture2D tex = new Texture2D(color.WidthPixels, color.HeightPixels, TextureFormat.BGRA32, false);
				tex.LoadRawTextureData(color.GetBufferCopy());
				tex.Apply();
				renderer.material.mainTexture = tex;
			}
		}
	}
	
    void CaptureSkeletonsFromCameraFrame()
	{
		using (var frame = tracker.PopResult())
		{
			Debug.LogFormat("{0} bodies found.", frame.NumBodies);
			if (frame.NumBodies > 0)
			{
				var bodyId = frame.GetBodyId(0);
				this.skeleton = frame.GetSkeleton(0);
				skeletons.Add(this.skeleton);
				for (var i = 0; i < (int)JointId.Count; i++)
				{
					var joint = this.skeleton.Joints[i];
					var pos = joint.Position;
					var rot = joint.Orientation;

					var v = new Vector3(pos[0], -pos[1], pos[2]) * 0.004f; 
					var r = new Quaternion(rot[1], rot[2], rot[3], rot[0]);

					string positionData = "pos " + (JointId)i + " " + pos[0] + " " + pos[1] + " " + pos[2];
					string rotationData = "rot " + (JointId)i + " " + rot[0] + " " + rot[1] + " " + rot[2] + " " + rot[3]; // Length 4

					print(positionData);
					//print("pos: " + (JointId)i + " " + v.ToString());
					print(rotationData);
					//print("rot " + (JointId)i + " " + r.ToString());

					//pos: ClavicleLeft -107.0713 -74.07419 837.8539
					//pos: ClavicleLeft (-107.1, 74.1, 837.9)
					//rot ClavicleLeft 0.7239407 -0.6615711 -0.01385375 -0.1950423
					//rot ClavicleLeft (-0.7, 0.0, -0.2, 0.7)

					//producerSendMessage(skeletons.Count + " " + positionData);
					//producerSendMessage(skeletons.Count + " " + rotationData);

					var obj = blockmanArray[i];
					obj.transform.SetPositionAndRotation(v, r);
				}
			}
		}
	}

	public void ToggleBlockman()
	{
		canUpdate = !canUpdate;
	}

	public void ToggleKafka()
	{
		enableKafka = !enableKafka;

	}

	void OnToggleValueChanged(bool isOn)
	{
		ColorBlock cb = recordPoseToggle.colors;
		cb.normalColor = Color.white; // blue;
		cb.highlightedColor = Color.white; // green;

		cb.selectedColor = Color.white;
		cb.pressedColor = Color.red;

		if (isOn)
		{
			cb.selectedColor = Color.red;
			cb.pressedColor = Color.white;
		}

		recordPoseToggle.colors = cb;
	}

	void ClearSkeletonsList()
	{
		skeletons.Clear();
	}
	
	void print(string msg)
	{
		Debug.Log(msg);
	}
	
    private void OnDisable()
    {
        //todo test if only called once at the end of the program, if so, renable the below
        print("DebugRenderer onDisable was called");
		//device.StopCameras();
		//k4a_device_close(device) here.
		if (tracker != null)
		{
			tracker.Dispose();
		}
		if (device != null)
		{
			device.StopCameras();
			device.Dispose();
		}
	}


}
