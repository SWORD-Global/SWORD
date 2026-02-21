lapstime=tic;
disp(['Read Sword Database version ' cas_sword_version ', zone: ' cas_sword_region])
disp(['File version: ' file_sword])
if ~exist(file_sword,'file')
    disp(['The Sword file indicated does not exist. Please check the name!'])
    disp('Press a key to continue ...')
    pause
    return
end
% Initialisation
rch_sos_inversed=[];
% Read Global attributes
x_min=ncreadatt(file_sword,'/','x_min'); % Bounding box of longitudes and latitudes included in a file
x_max=ncreadatt(file_sword,'/','x_max');
y_min=ncreadatt(file_sword,'/','y_min');
y_max=ncreadatt(file_sword,'/','y_max');
Name=ncreadatt(file_sword,'/','Name'); % 2 letters identifying the continent (ex: AF)
production_date=ncreadatt(file_sword,'/','production_date'); % Date when the files were generated
disp(['Production date : ' production_date])
% Read Centerlines
if read_ctl>0
    id_ctl=ncread(file_sword,'centerlines/cl_id'); % high-resolution centerline point id
    x_ctl=ncread(file_sword,'centerlines/x'); % longitude of the point ranging from 180°E to 180°W
    y_ctl=ncread(file_sword,'centerlines/y'); % latitude of the point ranging from 90°S to 90°N
    id_ctl_rch=ncread(file_sword,'centerlines/reach_id'); % id of each reach the high-resolution centerline point is associated with (1 to 4)
    id_ctl_nod=ncread(file_sword,'centerlines/node_id'); % id of each node the high-resolution centerline point is associated with (1 to 4)
    nb_ctl=size(id_ctl,1); % Number of centerline points
    % nb_object=nb_ctl;
    time_duration=toc(lapstime);
    disp(['Read Centerlines : ' num2str(time_duration) ' s'])
    lapstime=tic;
end
% Read Nodes
if read_nod>0
    id_nod=ncread(file_sword,'nodes/node_id'); % id of each node
    id_nod_ctl=ncread(file_sword,'nodes/cl_ids'); % minimum and maximum high-resolution centerline point ids along each node
    x_nod=ncread(file_sword,'nodes/x'); % longitude of each node ranging from 180°E to 180°W
    y_nod=ncread(file_sword,'nodes/y'); % latitude of each node, ranging from 90°S to 90°N
    length_nod=ncread(file_sword,'nodes/node_length'); % node length measured along the high-resolution centerline points (m)
    id_nod_rch=ncread(file_sword,'nodes/reach_id'); % id of the reach each node is associated with
    wse_nod_ave=ncread(file_sword,'nodes/wse'); % node average water surface elevation (m)
    wse_nod_var=ncread(file_sword,'nodes/wse_var'); % water surface elevation variance along the high-resolution centerline points used to calculate the average water surface elevation for each node (m^2)
    width_nod_ave=ncread(file_sword,'nodes/width'); % node average width (m)
    width_nod_var=ncread(file_sword,'nodes/width_var'); % width variance along the high-resolution centerline points used to calculate the average width for each node (m^2)
    n_chan_max_nod=ncread(file_sword,'nodes/n_chan_max'); % maximum number of channels for each node
    n_chan_mod_nod=ncread(file_sword,'nodes/n_chan_mod'); % mode of the number of channels for each node
    obstr_type_nod=ncread(file_sword,'nodes/obstr_type'); % Type of obstruction for each node based on GROD and HydroFALLS databases. Obstr_type values: 0 - No Dam, 1 - Dam, 2 - Channel Dam, 3 - Lock, 4 - Low Permeable Dam, 5 - Waterfall
    grod_id_nod=ncread(file_sword,'nodes/grod_id'); % The unique GROD ID for each node with obstr_type values 1-4
    % hfalls_id % The unique HydroFALLS ID for each node with obstr_type value 5
    dist_out_nod=ncread(file_sword,'nodes/dist_out'); % distance from the river outlet for each node (m)
    % wth_coef % coefficient that is multiplied by the width variable to inform the RiverObs search window for pixel cloud points
    % ext_dist_coef % coefficient that informs the maximum RiverObs search window for pixel cloud points
    facc_nod=ncread(file_sword,'nodes/facc'); % maximum flow accumulation value for each node (km^2)
    % lakeflag % GRWL water body identifier for each node: 0 – river, 1 – lake/reservoir, 2 – canal , 3 – tidally influenced river
    width_nod_max=ncread(file_sword,'nodes/max_width'); % maximum width value across the channel for each node that includes island and bar areas (m)
    % meander_length % length of the meander that a node belongs to, measured from beginning of the meander to its end in meters. For nodes longer than one meander, the meander length will represent the average length of all meanders belonging to the node
    % sinuosity % the total reach length the node belongs to divided by the Euclidean distance between the reach end points
    % manual_add % binary flag indicating whether the nodes was manually added to the public GRWL centerlines. These nodes were originally given a width = 1, but have since been updated to have the reach width values
    river_name_nod=ncread(file_sword,'nodes/river_name'); % all river names associated with a node. If there are multiple names for a node they are listed in alphabetical order and separated by a semicolon
    % edit_flag % numerical flag indicating the type of update applied to SWORD nodes from the previous version. Flag descriptions: 1 to 5 (cf csv file for Elisabeth)
    % trib_flag % binary flag indicating if a large tributary not represented in SWORD is entering a node. 0 - no tributary, 1 - tributary
    % From v17:
    if isequal(cas_sword_version,'v17')
        % path_freq % the number of times a node is traveled to get to any given headwater from the primary outlet.
        % path_order % unique values representing continuous paths from the river outlet to the headwaters. Values are unique within level two Pfafstetter basins. The lowest value is always the longest path from outlet to farthest headwater point in a connected river network. Higher path values branch off from the longest path value to other headwater points.
        % path_segs % unique values indicating continuous river segments between river junctions. Values are unique within level two Pfafstetter basins.
        % stream_order % stream order based on the log scale of the path frequency.
        main_side_nod=ncread(file_sword,'nodes/main_side'); % value indicating whether a node is on the main network (0), side network (1), or is a secondary outlet on the main network (2).
        % end_reach % value indicating whether a reach is a headwater (1), outlet (2), or junction (3) reach. A value of 0 means it is a normal main stem river reach. 
        % network % unique value for each connected river network. Values are unique within level two Pfafstetter basins.
    end
    nb_nod=size(id_nod,1);
    % nb_object=nb_nod;
    time_duration=toc(lapstime);
    disp(['Read Nodes : ' num2str(time_duration) ' s'])
    lapstime=tic;
end
% Read Reaches
if read_rch>0
    id_rch=ncread(file_sword,'reaches/reach_id'); % id of each reach
    id_rch_ctl=ncread(file_sword,'reaches/cl_ids'); % minimum and maximum high-resolution centerline point ids along each reach
    % We must compute id_rch_nod or index_rch_node (minimum and maximum nodes ids or indexes, along each reach (POM done in sword_compute))
    if isequal(cas_sword_version,'v17c')
        % From Sword v17c (by april-may 2026) a new variable has to be added (since in this version the nodes IDs will not always be named from dn to up, since some reaches will have their flow direction changed, but IDs names not changed to assure compatibility among the version D products timeseries):
        id_rch_nod=ncread(file_sword,'reaches/nodes_ids'); % first downstream and last upstream nodes ids along each reach (we assume the ids are contiguous and strictly monotonous, increasing or decreasing)
    end
    x_rch=ncread(file_sword,'reaches/x'); % longitude of the reach center ranging from 180°E to 180°W
    y_rch=ncread(file_sword,'reaches/y'); % latitude of the reach center ranging from 90°S to 90°N
    % x_max, x_min, y_max, y_min % Bounding box of longitudes and latitudes for a reach. Note that reaches may have overlapping boxes
    length_rch=ncread(file_sword,'reaches/reach_length'); % reach length measured along the high-resolution centerline points (m)
    n_rch_nod=ncread(file_sword,'reaches/n_nodes')'; % number of nodes associated with each reach
    wse_rch_ave=ncread(file_sword,'reaches/wse'); % reach average water surface elevation
    wse_rch_var=ncread(file_sword,'reaches/wse_var'); % water surface elevation variance along the high-resolution centerline points used to calculate the average water surface elevation for each reach
    width_rch_ave=ncread(file_sword,'reaches/width'); % reach average width
    width_rch_var=ncread(file_sword,'reaches/width_var'); % width variance along the high-resolution centerline points used to calculate the average width for each reach
    facc_rch=ncread(file_sword,'reaches/facc'); % maximum flow accumulation value for each reach (km^2)
    n_chan_max_rch=ncread(file_sword,'reaches/n_chan_max'); % maximum number of channels for each reach
    n_chan_mod_rch=ncread(file_sword,'reaches/n_chan_mod'); % mode of the number of channels for each reach
    obstr_type_rch=ncread(file_sword,'reaches/obstr_type'); % Type of obstruction for each reach based on GROD and HydroFALLS databases. Obstr_type values: 0 - No Dam, 1 - Dam, 2 - Channel Dam, 3 - Lock, 4 - Low Permeable Dam, 5 - Waterfall
    grod_id_rch=ncread(file_sword,'reaches/grod_id'); % The unique GROD ID for each reach with obstr_type values 1-4
    % hfalls_id % The unique HydroFALLS ID for each reach with obstr_type value 5
    slope_rch=ncread(file_sword,'reaches/slope'); % reach average slope calculated along the high-resolution centerline points (m/km)
    dist_out_rch=ncread(file_sword,'reaches/dist_out'); % distance from the river outlet for each reach
    n_rch_up_rch=ncread(file_sword,'reaches/n_rch_up'); % number of upstream reaches for each reach
    n_rch_dn_rch=ncread(file_sword,'reaches/n_rch_down'); % number of downstream reaches for each reach
    rch_id_up_rch=ncread(file_sword,'reaches/rch_id_up'); % reach ids of the upstream reaches
    rch_id_dn_rch=ncread(file_sword,'reaches/rch_id_dn'); % reach ids of the downstream reaches
    lakeflag_rch=ncread(file_sword,'reaches/lakeflag'); % GRWL water body identifier for each reach: 0 – river, 1 – lake/reservoir, 2 – canal , 3 – tidally influenced river
    % ice_flag % meteorological ice flag for each reach. Values include 0 – ice free, 1 – mixed, 2 – ice cover
    swot_obs=ncread(file_sword,'reaches/swot_obs'); % The maximum number of SWOT passes to intersect each reach during the 21 day orbit cycle
    swot_orbits=ncread(file_sword,'reaches/swot_orbits'); % A list of the SWOT orbit tracks that intersect each reach during the 21 day orbit cycle
    river_name_rch=ncread(file_sword,'reaches/river_name'); % all river names associated with a reach. If there are multiple names for a reach they are listed in alphabetical order and separated by a semicolon
    width_rch_max=ncread(file_sword,'reaches/max_width'); % maximum width value across the channel for each reach that includes island and bar areas
    % low_slope_flag % binary flag where a value of 1 indicates the reach slope is too low for effective discharge estimation
    % edit_flag % numerical flag indicating the type of update applied to a SWORD reach from the previous version. Flag descriptions: 1 to 5 (cf csv file for Elisabeth)
    % trib_flag % binary flag indicating if a large tributary not represented in SWORD is entering a reach. 0 - no tributary, 1 - tributary
    % From v17:
    if isequal(cas_sword_version,'v17')
        % path_freq % the number of times a reach is traveled to get to any given headwater from the primary outlet.
        % path_order % unique values representing continuous paths from the river outlet to the headwaters. Values are unique within level two Pfafstetter basins. The lowest value is always the longest path from outlet to farthest headwater point in a connected river network. Higher path values branch off from the longest path value to other headwater points.
        % path_segs % unique values indicating continuous river segments between river junctions. Values are unique within level two Pfafstetter basins.
        % stream_order % stream order based on the log scale of the path frequency.
        main_side_rch=ncread(file_sword,'reaches/main_side'); % value indicating whether a reach is on the main network (0), side network (1), or is a secondary outlet on the main network (2).
        % end_reach % value indicating whether a reach is a headwater (1), outlet (2), or junction (3) reach. A value of 0 means it is a normal main stem river reach. 
        % network % unique value for each connected river network. Values are unique within level two Pfafstetter basins.
    end
    % /reaches/area_fits : *h_break, *w_break, *h_variance, *w_variance,*hw_covariance, *h_err_stdev,*w_err_stdev, *h_w_nobs, *fit_coeffs, *med_flow_area
    % Variables for the 6 algo bellow do exist for constrained AND unconstrained cases
    % /reaches/discharge_models/[unconstrained][constrained]/MetroMan :
    % *Abar, *Abar_stdev, *ninf, *ninf_stdev, *p, *p_stdev, *ninf_p_cor, *p_Abar_cor, *ninf_Abar_cor, *sbQ_rel
    % /reaches/discharge_models/[unconstrained][constrained]/BAM :
    % *Abar, *n, *sbQ_rel
    % /reaches/discharge_models/[unconstrained][constrained]/HiVDI :
    % *Abar, *alpha, *beta, *sbQ_rel
    % /reaches/discharge_models/[unconstrained][constrained]/MOMMA :
    % *B, *H, *Save, *sbQ_rel
    % /reaches/discharge_models/[unconstrained][constrained]/SADS :
    % *Abar, *n, *sbQ_rel
    % /reaches/discharge_models/[unconstrained][constrained]/SIC4DVar :
    % *Abar, *n, *sbQ_rel
    nb_rch=size(id_rch,1);
    % nb_object=nb_rch;
    time_duration=toc(lapstime);
    disp(['Read Reaches : ' num2str(time_duration) ' s'])
    lapstime=tic;
end
% Gestion des id_nod avec codes Pfafstetter pour avoir les codes
% individuels des divers niveaux : Bassin, Reach, Node, Type
if read_nod>0
    % Rmk: ces vecteurs ne semblent pas être utilisés, on pourrait ne pas les créer (POM 19/12/2023)
    id_nod_b=zeros(1,nb_nod);
    id_nod_r=zeros(1,nb_nod);
    id_nod_n=zeros(1,nb_nod);
    id_nod_t=zeros(1,nb_nod);
    for ii=1:nb_nod
        % Passage en chaine
        id_nod_s=num2str(id_nod(ii));
        id_nod_s_b=id_nod_s(2:6); % Bassin
        id_nod_s_r=id_nod_s(7:10); % Reach
        id_nod_s_n=id_nod_s(11:13); % Node
        id_nod_s_t=id_nod_s(14:14); % Type
        % Passage en numérique
        id_nod_b(ii)=str2double(id_nod_s_b);
        id_nod_r(ii)=str2double(id_nod_s_r);
        id_nod_n(ii)=str2double(id_nod_s_n);
        id_nod_t(ii)=str2double(id_nod_s_t);
    end
end
% Gestion des id_rch avec codes Pfafstetter
if read_rch>0
    id_rch_c=zeros(1,nb_rch);
    id_rch_b=zeros(1,nb_rch);
    id_rch_r=zeros(1,nb_rch);
    id_rch_t=zeros(1,nb_rch);
    id_rch_filter=ones(1,nb_rch);
    for ii=1:nb_rch
        % Passage en chaine
        id_rch_s=num2str(id_rch(ii));
        id_rch_s_c=id_rch_s(1); % Continent
        id_rch_s_b=id_rch_s(2:6); % Bassin
        id_rch_s_r=id_rch_s(7:10); % Reach
        id_rch_s_t=id_rch_s(11:11); % Type
        % Passage en numérique
        id_rch_c(ii)=str2double(id_rch_s_c);
        id_rch_b(ii)=str2double(id_rch_s_b);
        id_rch_r(ii)=str2double(id_rch_s_r);
        id_rch_t(ii)=str2double(id_rch_s_t);
        % Remplissage du vecteur id_rch_filter permettant éventuellement de limiter la vérification globale ou locale à des
        % bassins. Pour la globale c'est particulièrement intéressant pour limiter la zone à vérifier
        if opt_sword_valid_limit_pfaf_c>0
            if id_rch_c(ii)~=opt_sword_valid_limit_pfaf_c || ~ismember(id_rch_b(ii),opt_sword_valid_limit_pfaf_b)
                id_rch_filter(ii)=0;
            end
        end
    end
end
time_duration=toc(lapstime);
disp(['Treatment Ids : ' num2str(time_duration) ' s'])

% cas_sword_region='eu';
% cas_sword_type='sword';
% cas_sword_version_dir='v17e'; % 'v07', 'v09', 'v11', 'v12', 'v14', 'v15', 'v15_patch', 'v16', 'v17a', 'v17b', 'v17c', 'v17d', 'v17e', 'v17' (Pour Confluence on utilise encore v11, puis v15 voire v15_patch, puis v16)
% cas_sword_version_file='v17';
% dirdropbox='E:\Dropbox\';
% subdir_sword='Irstea\Projets\2021_SWOT\SWORD\';
% name_file_sword=[cas_sword_region '_' cas_sword_type '_' cas_sword_version_file '.nc'];
% dir_sword=[dirdropbox subdir_sword 'Reaches_Nodes_' cas_sword_version_dir '\netcdf\'];
% file_sword=[dir_sword name_file_sword];
% info1=ncinfo(file_sword);
% disp(info1)
% ncdisp(file_sword);
