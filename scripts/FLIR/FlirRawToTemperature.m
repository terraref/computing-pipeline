[FileName,PathName,FilterIndex] = uigetfile('*.bin');

fileID = fopen([PathName FileName]);
RawPixelValue = fread(fileID,[640,480],'uint16');

figure(1),title('Raw Camera Value')
RawVector=reshape(RawPixelValue,1,640*480);
hist(RawVector,65025);
set(gca,'yscale',"log")
xlabel ("raw pixel value")
ylabel ("n")


% Constant camera-specific parameters determined by FLIR 

% Plank constant - Flir

% R	Planck constant		function of integration time and wavelength)
% B	Planck constant		function of wavelength
% F	Planck constant		positive value (0 - 1)
% J0	global offset
% J1	global gain	

R  = 15976.1;
B  = 1417.3;
F  = 1.00;
J0 = 3597;
J1 = 73.549;

% Constant Atmospheric transmission parameter by Flir

X  = 1.9;
a1 = 0.006569;
b1 = -0.002276;
a2 = 0.01262;
b2 = -0.00667;

% Constant for VPD computation (sqtrH2O)				
H2O_K1 = 1.56E+00;
H2O_K2 = 6.94E-02;
H2O_K3 = -2.78E-04;
H2O_K4 = 6.85E-07;

%  Environmental factors
% According to FLIR, atmospheric absorption under 10m object distance can be
% neglected, expecially under dry desert climate
			
	% H = Relative Humidity from the gantry  (0 - 1)      			
	% T = air temperature in degree Celsius	from the gantry	
	% D = ObjectDistance - camera/canopy (m)
	% E = object emissivity, vegetation is around 0.98, bare soil around 0.93...
	% AmbTemp or reflective Temp (K): NEED TO BE MEASURED BEFORE/AFTER IMAGE ACQUISITION
	% AtmTemp or air temp (K)			
	% ExtOpticsTemp (K) = AtmTemp
	% by default: AmbTemp = AtmTemp = ExtOpticsTemp

H = 0.1; % gantry value
T = 22.0; % gantry value
D = 2.5;
E = 0.98;

AmbTemp = T + 273.15;  % Temperature at canopy level assumed to Atmospheric temperature
AtmTemp = T + 273.15;
ExtOpticsTemp = 287.95; % to be specified and not used here


% Theoretical object radiation = Raw pixel values (raw_pxl_val)
	% Flir image in 16 integer unsigned format




% Step 1: Atmospheric transmission - correction factor from air temp, relative humidity and distance sensor-object;

%  Vapour pressure deficit call here sqrtH2O => convert relative humidity and air temperature in VPD - mmHg - 1mmHg=0.133 322 39 kPa 

H2OInGperM2 = H*exp(H2O_K1 + H2O_K2*T + H2O_K3*(T.^2) + H2O_K4*(T.^3));

%  Atmospheric transmission correction: tao
a1b1sqH2O = (a1+b1*sqrt(H2OInGperM2));
a2b2sqH2O = (a2+b2*sqrt(H2OInGperM2));
exp1 = exp(-sqrt(D/2)*a1b1sqH2O);
exp2 = exp(-sqrt(D/2)*a2b2sqH2O); 
    
tao = X*exp1 + (1-X)*exp2; % Atmospheric transmission factor


% Step 2: Step2: Correct raw pixel values from external factors; 

	% General equation : Total Radiation = Object Radiation + Atmosphere Radiation + Ambient Reflection Radiation

	
% Object Radiation: obj_rad 
	% obj_rad = Theoretical object radiation * emissivity * atmospheric transmission
	% Theoretical object radiation: raw_pxl_val
	
obj_rad = RawPixelValue.* E * tao; %  FOR EACH PIXEL

% Atmosphere Radiation: atm_rad
	% atm_rad= (1 - atmospheric transmission) * Theoretical atmospheric radiation
	% Theoretical atmospheric radiation: theo_atm_rad
theo_atm_rad = (R*J1/(exp(B/AtmTemp)-F)) +J0;

atm_rad = repmat((1 - tao).* theo_atm_rad,size(RawPixelValue));

% Ambient Reflection Radiation: amb_refl_rad
	% amb_refl_rad = (1 - emissivity) * atmospheric transmission * Theoretical Ambient Reflection Radiation
	% Theoretical Ambient Reflection Radiation: theo_amb_refl_rad
theo_amb_refl_rad = (R*J1/(exp(B/AmbTemp)-F)) + J0;

amb_refl_rad = repmat((1 - E) * tao * theo_amb_refl_rad,size(RawPixelValue));

% Total Radiation: corr_pxl_val
corr_pxl_val= obj_rad + atm_rad + amb_refl_rad; % FOR EACH PIXEL


% Step 3:RBF equation: transformation of pixel intensity in radiometric temperature from raw values or corrected values

	%  in kelvin
pxl_temp = B./log(R./(corr_pxl_val - J0).*J1+F); % FOR EACH PIXEL

	%  in degree Celsius
pxl_temp = B./log(R./(corr_pxl_val - J0).*J1+F) - 273.15; % FOR EACH PIXEL



figure(2), title('Temperature in 0.01 °C')
TempVector=reshape(pxl_temp,1,640*480);
hist(TempVector,1000);
set(gca,'yscale',"log")
xlabel ("temperature")
ylabel ("n")


 %colormap('hot')
%figure(1),imagesc(At),colorbar, axis off