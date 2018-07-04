package org.omlstreaming.performance;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Random;

public class GlobalFeatureServer implements Runnable {

  private GlobalFeatureProfilerDebug debugFlag = new GlobalFeatureProfilerDebug();

  private Socket socketClient = null;

  private static int numSamples = 8000000;

  private final int evtBurstSize = 1000; // events
  private final int evtBurstLatency = 50; // ms

  private Double[] dataStream = {-8.21866499e+00, -8.69290703e+00, -8.96950859e+00, -9.93838064e+00,
      -9.08787707e+00, -1.10195558e+01, -9.38208704e+00, -4.64346475e+00, -9.18292059e+00,
      -9.03268919e+00, -7.85034791e+00, -8.29081267e+00, -8.91820158e+00, -6.71135376e+00,
      -9.06715235e+00, -9.60792315e+00, -9.30163141e+00, -1.12282097e+01, -1.02967798e+01,
      -7.50497503e+00, -5.56603439e+00, -7.64072684e+00, -8.84817952e+00, -1.11390369e+01,
      -9.31384379e+00, -6.30863049e+00, -1.44643122e+01, -9.16821316e+00, -9.16013121e+00,
      -8.68241196e+00, -8.32944762e+00, -5.45833307e+00, -8.41376793e+00, -9.79005792e+00,
      -1.12049362e+01, -9.71715819e+00, -7.47947856e+00, -9.34636158e+00, -6.81169608e+00,
      -5.16312333e+00, -9.56612906e+00, -7.36318638e+00, -7.92434025e+00, -9.43484675e+00,
      -8.30848213e+00, -1.15728481e+01, -7.73431210e+00, -1.24277743e+01, -7.18907894e+00,
      -7.99027683e+00, -1.03247862e+01, -1.19711574e+01, -8.23179876e+00, -9.33633535e+00,
      -1.09823802e+01, -1.24053151e+01, -1.35132756e+01, -1.15086208e+01, -1.04877456e+01,
      -1.20192577e+01, -8.42966242e+00, -7.29235330e+00, -9.17716058e+00, -8.63903036e+00,
      -5.82192301e+00, -5.13147734e+00, -6.75643038e+00, -8.44483708e+00, -8.68553250e+00,
      -1.09426240e+01, -4.39203280e+00, -1.21458671e+01, -9.95089379e+00, -1.09676950e+01,
      -1.06338245e+01, -8.25070779e+00, -8.88920566e+00, -6.30269156e+00, -9.94377429e+00,
      -1.01013914e+01, -9.16187573e+00, -7.07147049e+00, -8.19150612e+00, -8.80277912e+00,
      -7.95534978e+00, -1.51043365e+01, -7.94537201e+00, -1.09919226e+01, -9.44092616e+00,
      -9.00278970e+00, -5.99009551e+00, -9.73234800e+00, -8.61255349e+00, -4.94527403e+00,
      -1.00966103e+01, -8.85429949e+00, -1.07294599e+01, -9.62435911e+00, -7.24810513e+00,
      -9.22108047e+00, -5.67671322e+00, -7.87441864e+00, -1.04689151e+01, -1.28444176e+01,
      -6.99204532e+00, -6.35836795e+00, -7.69942327e+00, -9.93298935e+00, -8.71579514e+00,
      -1.17330424e+01, -1.29816325e+01, -1.01519573e+01, -6.12093111e+00, -8.00504489e+00,
      -6.84387244e+00, -8.82573109e+00, -7.38378590e+00, -1.21031228e+01, -1.04127080e+01,
      -1.23442245e+01, -8.94143199e+00, -8.53116514e+00, -1.09239806e+01, -9.21407143e+00,
      -9.04382862e+00, -1.19868949e+01, -9.28293649e+00, -9.75159607e+00, -1.05368874e+01,
      1.95394631e+01, 1.84652932e+01, 1.87755384e+01, 1.72068278e+01, 1.85132270e+01,
      1.94598251e+01, 1.80297555e+01, 1.82087818e+01, 1.81173187e+01, 1.89376609e+01,
      1.81531755e+01, 1.87211419e+01, 1.88050362e+01, 1.84641421e+01, 1.88742900e+01,
      1.88779200e+01, 1.79037430e+01, 1.80351971e+01, 1.77278648e+01, 1.84015420e+01,
      1.80625393e+01, 1.87974489e+01, 1.77749207e+01, 1.86093708e+01, 1.84428228e+01,
      1.73965589e+01, 1.73436538e+01, 1.86656302e+01, 1.86311554e+01, 1.86353966e+01,
      1.84272094e+01, 1.81541404e+01, 1.87490086e+01, 1.81234574e+01, 1.90689807e+01,
      1.90997868e+01, 1.81134416e+01, 1.88283605e+01, 1.86140314e+01, 1.79091938e+01,
      1.89235805e+01, 1.87184960e+01, 1.90387462e+01, 1.79698989e+01, 1.81949306e+01,
      1.85770650e+01, 1.78331731e+01, 1.82270821e+01, 1.83674955e+01, 1.90601736e+01,
      1.76820518e+01, 1.84396108e+01, 1.87921545e+01, 1.83065834e+01, 1.90059699e+01,
      1.82638814e+01, 1.80751268e+01, 1.85232261e+01, 1.84103581e+01, 1.84063798e+01,
      1.83268344e+01, 1.75058469e+01, 1.71241229e+01, 1.77075197e+01, 1.79460506e+01,
      1.88073770e+01, 1.90616838e+01, 1.75764868e+01, 1.82064531e+01, 1.82028673e+01,
      1.91108288e+01, 1.77935072e+01, 1.90297554e+01, 1.88866864e+01, 1.84382452e+01,
      1.73466037e+01, 1.70670896e+01, 1.87048422e+01, 1.77521166e+01, 1.81316716e+01,
      1.73767647e+01, 1.84206698e+01, 1.85903435e+01, 1.80121433e+01, 1.80166363e+01,
      1.81406774e+01, 1.79019408e+01, 1.86866332e+01, 1.77587859e+01, 1.78042697e+01,
      1.71061370e+01, 1.77221638e+01, 1.81748943e+01, 1.74220099e+01, 1.90228359e+01,
      1.89255031e+01, 1.79139561e+01, 1.83230169e+01, 1.68409206e+01, 1.89837750e+01,
      1.79042314e+01, 1.82119282e+01, 1.71260747e+01, 1.79459513e+01, 1.70080805e+01,
      1.75442309e+01, 1.77350807e+01, 1.86401250e+01, 1.77228370e+01, 1.97199249e+01,
      1.86357143e+01, 1.93301609e+01, 1.83436160e+01, 1.82484234e+01, 1.80556684e+01,
      1.82673116e+01, 1.78259688e+01, 1.78558771e+01, 1.82604852e+01, 1.83327618e+01,
      1.73566740e+01, 1.85204807e+01, 1.87261017e+01, 1.73941748e+01, 1.87555511e+01,
      1.79881867e+01, 1.90640604e+01, 1.78241017e+01, 1.86046481e+01, 1.79590530e+01,
      1.81293923e+01, 1.75786518e+01, 1.90350305e+01, 1.85341351e+01, 1.78953375e+01,
      1.78029059e+01, 1.80400155e+01, 1.89335641e+01, 1.82309324e+01, 1.79158501e+01,
      1.78477554e+01, 1.81658026e+01, 1.77752127e+01, 1.83419017e+01, 1.88986178e+01,
      1.82889144e+01, 1.86784719e+01, 1.83694234e+01, 1.79882126e+01, 1.79329010e+01,
      1.78411207e+01, 1.78448357e+01, 1.92539124e+01, 1.72162959e+01, 1.78458714e+01,
      1.92197815e+01, 1.82898431e+01, 1.78732869e+01, 1.86749552e+01, 1.84867301e+01,
      1.80265023e+01, 1.87875488e+01, 1.87572877e+01, 1.72874911e+01, 1.89383891e+01,
      1.85637280e+01, 1.79870039e+01, 1.87051543e+01, 1.87317799e+01, 1.87173670e+01,
      1.80975108e+01, 1.84740487e+01, 1.80309462e+01, 1.86858799e+01, 1.85775471e+01,
      1.82246615e+01, 1.84268481e+01, 1.78553871e+01, 1.80488297e+01, 8.88665034e+00,
      9.00480653e+00, 1.17667902e+01, 1.05697704e+01, 8.45535740e+00, 7.29626868e+00,
      8.52479842e+00, 9.44594921e+00, 1.06145619e+01, 7.94436759e+00, 8.22528940e+00,
      1.23688307e+01, 8.98152639e+00, 9.09444845e+00, 7.34657119e+00, 1.02432719e+01,
      9.43412823e+00, 9.16043275e+00, 1.12683381e+01, 9.87152348e+00, 8.55443371e+00,
      7.60848282e+00, 7.27043553e+00, 8.19520925e+00, 1.04280226e+01, 7.78249171e+00,
      8.42632235e+00, 1.09201983e+01, 8.76050909e+00, 7.60746587e+00, 1.15718218e+01,
      9.10440382e+00, 9.42683636e+00, 1.15098732e+01, 1.02723911e+01, 1.01509528e+01,
      1.13971600e+01, 8.36955777e+00, 7.32440814e+00, 9.56593543e+00, 9.43187950e+00,
      1.18855754e+01, 9.84947867e+00, 8.08617820e+00, 8.17066840e+00, 8.65482456e+00,
      1.05256504e+01, 9.19855503e+00, 1.13772583e+01, 9.60156878e+00, 1.30096922e+01,
      1.02659997e+01, 8.61306787e+00, 9.40075695e+00, 9.99715822e+00, 9.70178228e+00,
      8.31672042e+00, 1.07170167e+01, 7.64966902e+00, 1.17844113e+01, 7.13407688e+00,
      6.19706698e+00, 8.45323617e+00, 1.02076777e+01, 7.82825291e+00, 9.98924026e+00,
      1.02581100e+01, 1.04235439e+01, 9.21588771e+00, 8.16563037e+00, 1.23376401e+01,
      1.03039239e+01, 8.13487499e+00, 8.23302176e+00, 1.00563105e+01, 1.01939531e+01,
      9.90863784e+00, 8.99033942e+00, 9.31637447e+00, 1.02471153e+01, 8.13875911e+00,
      8.41463964e+00, 1.28209093e+01, 1.17479387e+01, 1.08426019e+01, 1.18859788e+01,
      1.05272352e+01, 1.07296061e+01, 1.20244249e+01, 1.02532285e+01, 9.57615858e+00,
      9.63589921e+00, 9.32717198e+00, 8.97632247e+00, 9.51800988e+00, 1.14645595e+01,
      9.32336043e+00, 8.54137420e+00, 1.08713993e+01, 1.24906468e+01, 1.05556687e+01,
      9.57628887e+00, 9.15633885e+00, 1.13406840e+01, 9.42166189e+00, 9.70024028e+00,
      9.61520239e+00, 9.98609177e+00, 1.18222822e+01, 1.10725121e+01, 9.02028447e+00,
      8.32440727e+00, 9.56311762e+00, 9.40271599e+00, 1.09473478e+01, 9.59174599e+00,
      8.88558660e+00, 1.01691934e+01, 1.19652821e+01, 8.81226993e+00, 1.06780587e+01,
      8.19152268e+00, 9.87432726e+00, 1.13944908e+01, 7.40705960e+00, 9.07842508e+00,
      9.54294735e+00, 9.80373528e+00, 7.69130834e+00, 1.06943230e+01, 1.01345388e+01,
      9.71844831e+00, 1.29395505e+01, 1.04378549e+01, 7.52695697e+00, 1.10217575e+01,
      7.58254771e+00, 1.16003084e+01, 1.04972580e+01, 9.90832820e+00, 9.02936707e+00,
      9.91677874e+00, 1.00524783e+01, 1.04283089e+01, 1.19593265e+01, 1.19672809e+01,
      7.81935214e+00, 1.21966349e+01, 1.18585600e+01, 8.82930832e+00, 4.54197848e+00,
      3.96136851e+00, 7.12316458e+00, 6.68647220e+00, 4.35072032e+00, 4.24054572e+00,
      5.28155656e+00, 6.96890675e+00, 5.33754679e+00, 4.82235064e+00, 8.71565365e+00,
      5.31468573e+00, 2.30314521e+00, 3.97395246e+00, 5.85711742e+00, 7.03360873e+00,
      3.64769734e+00, 7.48555713e+00, 4.33980512e+00, 5.52913765e+00, 5.16120675e+00,
      3.40584973e+00, 4.47129034e+00, 4.55024546e+00, 4.78059035e+00, 3.66453299e+00,
      6.15285470e+00, 4.80403031e+00, 5.08143351e+00, 1.85714172e+00, 6.41673773e+00,
      7.90311719e+00, 5.07718816e+00, 6.31597007e+00, 2.58709965e+00, 6.61254308e+00,
      3.22654396e+00, 5.22088030e+00, 1.55098454e+00, 5.44462959e+00, 3.06493464e+00,
      4.86384191e+00, 5.78706163e+00, 3.44300032e+00, 3.72139790e+00, 5.21950215e+00,
      5.81139517e+00, 2.98173233e-01, 5.26410401e+00, 8.06047533e+00, 4.32296630e+00,
      3.78696208e+00, 4.62266480e+00, 4.74805562e+00, 2.79742059e+00, 4.68180154e+00,
      4.69734237e+00, 3.37524013e+00, 4.34881706e+00, 4.68810670e+00, 5.89001995e+00,
      6.40111037e+00, 5.59523149e+00, 5.96118172e+00, 3.82754240e+00, 3.96456331e+00,
      5.44499890e+00, 5.13604085e+00, 4.67136193e+00, 2.08153011e+00, 4.46006171e+00,
      2.51224412e+00, 3.00235318e+00, 3.67415673e+00, 6.21409169e+00, 4.76791984e+00,
      5.39838057e+00, 3.78218066e+00, 6.04344264e+00, 5.26536678e+00, 4.83691137e+00,
      3.10013781e+00, 2.77553776e+00, 3.17823477e+00, 4.80989748e+00, 9.04273977e+00,
      4.56026000e+00, 5.30585617e+00, 3.45725742e+00, 4.05983544e+00, 6.15161731e-01,
      7.18318436e+00, 6.74147748e+00, 5.48277994e+00, 7.59369573e+00, 2.60868401e+00,
      5.39488994e+00, 6.46956707e+00, 7.84084327e+00, 6.28049127e+00, 5.39018330e+00,
      6.26454214e+00, 3.55717562e+00, 4.18548911e+00, 3.62811992e+00, 4.26523239e+00,
      5.81066577e+00, 5.13426528e+00, 3.81117438e+00, 4.36089994e+00, 6.87675875e+00,
      6.16940075e+00, -1.98002775e+00, -2.18060481e+00, -2.28962143e+00, -2.15982880e+00,
      -2.53873707e+00, -2.36198031e+00, -2.09121885e+00, -2.25344657e+00, -1.97373432e+00,
      -2.20912379e+00, -1.97640252e+00, -2.29761288e+00, -1.90649488e+00, -2.27220919e+00,
      -2.27220767e+00, -2.96448880e+00, -1.78581887e+00, -2.32824400e+00, -2.42327587e+00,
      -2.13985478e+00, -1.87627040e+00, -2.44411980e+00, -2.61764409e+00, -1.81650203e+00,
      -2.23852092e+00, -3.29293499e+00, -2.61710492e+00, -2.35145207e+00, -2.00851777e+00,
      -2.34010775e+00, -2.21264825e+00, -2.24287307e+00, -2.06544316e+00, -2.41383807e+00,
      -2.41026673e+00, -2.32491939e+00, -2.54474075e+00, -1.83748033e+00, -2.72050684e+00,
      -2.28555710e+00, -1.98800690e+00, -1.76687727e+00, -1.98555766e+00, -2.30219912e+00,
      -1.62362156e+00, -2.23730703e+00, -2.51363180e+00, -2.37398608e+00, -2.40380144e+00,
      -2.44908992e+00, -1.85958891e+00, -2.37367048e+00, -2.09145738e+00, -2.31708490e+00,
      -2.38619466e+00, -1.91043836e+00, -2.54734569e+00, -1.88674803e+00, -2.26607550e+00,
      -2.22973625e+00, -2.54906900e+00, -2.03932368e+00, -2.22732534e+00, -2.13405039e+00,
      -2.01640064e+00, -1.92514836e+00, -2.84449166e+00, -2.11852165e+00, -2.55342373e+00,
      -2.80679735e+00, -2.04966000e+00, -2.36220754e+00, -2.57426021e+00, -2.88863009e+00,
      -2.41905589e+00, -2.39810451e+00, -2.10268178e+00, -2.35415532e+00, -2.30883108e+00,
      -2.18106904e+00, -1.81135631e+00, -2.33151715e+00, -1.88525269e+00, -2.67764364e+00,
      -2.50750191e+00, -2.05119019e+00, -2.31944285e+00, -2.00848654e+00, -2.07498570e+00,
      -2.52432826e+00, -2.48500754e+00, -2.26434066e+00, -2.46441725e+00, -2.59384899e+00,
      -2.00203412e+00, -1.94631943e+00, -2.26099169e+00, -2.43051726e+00, -1.84049508e+00,
      -2.02387908e+00, -2.41438236e+00, -2.28743631e+00, -2.23411649e+00, -1.78062616e+00,
      -2.10547240e+00, -2.64729717e+00, -2.28723970e+00, -2.50167681e+00, -2.37571663e+00,
      -2.27244839e+00, -2.21660116e+00, -2.18114929e+00, -1.67843780e+00, -2.32534700e+00,
      -2.05476202e+00, -2.95123832e+00, -2.09465642e+00, -2.31203729e+00, -2.30509982e+00,
      -2.25352442e+00, -1.96436705e+00, -2.12442248e+00, -2.52554090e+00, -2.32644008e+00,
      -2.65309199e+00, -7.94262601e+00, -7.90705538e+00, -7.97404426e+00, -7.94288568e+00,
      -7.95420698e+00, -7.95122209e+00, -7.93312488e+00, -7.93449366e+00, -7.92880644e+00,
      -7.90797408e+00, -7.94516928e+00, -7.96242897e+00, -7.94178356e+00, -7.89487162e+00,
      -7.92782418e+00, -7.94565171e+00, -7.90722341e+00, -7.97697808e+00, -7.90986325e+00,
      -7.94371616e+00, -7.90735378e+00, -7.91745919e+00, -7.92620707e+00, -7.93291498e+00,
      -7.90442918e+00, -7.90939635e+00, -7.87640627e+00, -7.95062685e+00, -7.89087307e+00,
      -7.94362019e+00, -7.89668583e+00, -7.95051231e+00, -7.93654044e+00, -7.93852682e+00,
      -7.90404929e+00, -7.93792527e+00, -7.90344819e+00, -7.92369940e+00, -7.95419857e+00,
      -7.93160395e+00, -7.90476754e+00, -7.91946996e+00, -7.92663326e+00, -7.94352639e+00,
      -7.91920319e+00, -7.92968707e+00, -7.94423513e+00, -7.89327956e+00, -7.90584516e+00,
      -7.95051508e+00, -7.91955243e+00, -7.92746215e+00, -7.92261741e+00, -7.96540029e+00,
      -7.91486281e+00, -7.95205142e+00, -7.94631538e+00, -7.95648678e+00, -7.95394388e+00,
      -7.95102919e+00, -7.88261960e+00, -7.92189232e+00, -7.99382645e+00, -7.91846580e+00,
      -7.90035595e+00, -7.93067251e+00, -7.89098255e+00, -7.94106347e+00, -7.89747876e+00,
      -7.94782060e+00, -7.95064651e+00, -7.90353808e+00, -7.88076131e+00, -7.90787272e+00,
      -7.92489090e+00, -7.90464947e+00, -7.93955860e+00, -7.95602282e+00, -7.92420776e+00,
      -7.93589473e+00, -7.92289830e+00, -7.92844428e+00, -7.95528912e+00, -7.91182691e+00,
      -7.90071663e+00, -7.96115237e+00, -7.94820309e+00, -7.96114197e+00, -7.95279266e+00,
      -7.98034339e+00, -7.91276097e+00, -7.92496293e+00, -7.94730379e+00, -7.90690545e+00,
      -7.93134966e+00, -2.16031286e+00, -2.29456658e+00, -3.69386306e+00, -2.89973465e-01,
      -1.71511556e+00, -1.67414970e+00, -1.04476199e+00, 2.44005215e-02, -2.96330700e-01,
      -3.24928369e+00, -2.31700594e+00, 1.65584886e-01, -7.91162087e-02, -1.11937985e+00,
      -1.03341365e+00, -1.23839929e+00, -1.67146831e+00, -6.02712971e-01, -2.24252134e+00,
      -2.25670848e+00, -2.22299628e-01, -1.40060605e+00, -1.17389975e+00, -9.36356857e-01,
      -1.75421711e+00, -1.15400151e+00, -6.05585463e-01, -5.33962288e-01, -1.17863613e+00,
      -1.04931468e+00, -2.64454537e+00, -1.48361108e+00, 9.73798572e-01, -3.69708576e+00,
      -9.26918496e-01, -1.50106971e+00, -4.05297427e+00, -3.01719624e+00, 8.68900050e-01,
      -3.13947974e+00, -2.87966476e+00, -3.44386157e+00, -1.00129079e+00, -2.23827554e+00,
      -1.63196376e+00, -2.11090810e-01, -1.68285177e+00, -2.32226355e-01, -3.15901889e+00,
      -2.13472046e+00, -1.92713148e+00, -1.32214055e+00, -1.99754504e+00, -1.70818205e+00,
      -1.58489727e+00, -2.49658709e+00, -2.54614823e+00, -1.28879064e+00, -4.07451556e+00,
      -2.03091828e+00, -2.65670874e+00, -2.38942641e+00, -2.61693321e+00, -6.20243231e-01,
      1.19267017e+00, -1.98259935e+00, -6.65944271e+00, -2.34620522e+00, -3.34238678e+00,
      -1.24308225e+00, -3.40505939e+00, -2.33427762e+00, -1.97637683e+00, -2.79406365e+00,
      -1.51095792e-02, -1.83147978e+00, -2.77459672e+00, -2.10130658e+00, -2.26509460e+00,
      -2.51312239e+00, -2.41904985e+00, 1.40189714e-01, -3.22535386e+00, -8.17508114e-02,
      -3.00968499e+00, -2.55020771e-01, -5.77677696e-01, -2.29707981e+00, -2.76026705e-01,
      -2.35624318e+00, -2.35930307e+00, -1.03991431e+00, -1.92388972e+00, -3.32327126e+00,
      -1.02164101e+00, -1.36396845e+00, -2.30645604e+00, -1.47914803e+00, -1.88959197e+00,
      -2.41810234e+00, -2.53052218e+00, -1.22726053e+00, -2.12900074e+00, -1.69249730e+00,
      -1.71356361e+00, -1.21853473e+00, -2.10042633e+00, -9.28114284e-01, -1.32568428e+00,
      -3.90686100e+00, -2.18033067e+00, -2.74570560e+00, -1.74725273e+00, -6.12509145e-01,
      -1.74297983e+00, -2.46794648e+00, -4.06722755e+00, -1.90365346e+00, -2.73125671e+00,
      -3.85943144e+00, -2.23263234e+00, -1.76406884e+00, -9.50890996e-01, -1.54366733e+00,
      -2.83189284e-01, -1.66433737e+00, -1.32435322e+00, -1.83203202e+00, -2.18029790e+00,
      -1.26541141e+00, 8.77773496e-01, -3.11553987e+00, -2.49409622e+00, -2.06483470e+00,
      -8.21827682e-01, -9.98849716e-01, -9.18748685e-01, -1.39017489e+00, -1.95384402e+00,
      -4.79379219e-01, -9.73155743e-01, -4.20102124e+00, -2.66454162e+00, -2.56214986e+00,
      -3.39734414e+00, 1.25687643e-01, -1.52100526e+00, -2.60554533e+00, -2.04038257e+00,
      -2.39965176e+00, -4.50581925e+00, -3.19015702e+00, -2.61964735e+00, -1.55761279e+00,
      -2.65927825e+00, -3.82547019e+00, -1.79618436e+00, -2.91350114e+00, -7.08744288e-01,
      -2.24987198e+00, -3.16296563e+00, -1.81840029e+00, -1.87492488e+00, -7.07722540e-01,
      4.41502366e-02, -1.77631898e-01, -3.64413156e+00, -1.74483572e+00, -1.13570095e+00,
      -2.52112383e+00, -2.17173854e-01, -5.93808667e-01, -1.87216298e+00, -2.59174848e+00,
      -1.35082839e+00, -3.11461303e+00, -2.27173058e+00, -3.47204712e+00, -1.13471918e+00,
      3.07185641e-01, -7.48939596e-02, -2.27573665e+00, -1.65463206e+00, -1.74694913e-01,
      -2.19236148e+00, -3.46562642e-01};

  public GlobalFeatureServer(Socket socket) {
    socketClient = socket;
  }

  static int elems;

  public static void main(String[] args) throws IOException {

    // get the testing configuration
    GFETestingProperties testConf = new GFETestingProperties();
    ArrayList<String> paramTest = testConf.getPropValues();
    int commPort = Integer.valueOf(paramTest.get(3));
    elems = Integer.parseInt(args[0]);

    ServerSocket listener = new ServerSocket(commPort);
    try {
      while (true) {
        Socket socket = listener.accept();
        GlobalFeatureServer hand = new GlobalFeatureServer(socket);

        (new Thread(hand)).start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        listener.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

  }

  private static Double[] sigGen(int freq, int ts) {

    int sigBound = 10;
    Double[] sin = new Double[ts * numSamples];
    double samplingInterval = (double) (numSamples / freq);
    for (int i = 0; i < sin.length; i++) {
      sin[i] = Math.sin((2.0 * Math.PI * i) / samplingInterval) * sigBound;
    }

    return sin;
  }

  @Override
  public void run() {

    int counter = 0;
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      PrintWriter out = new PrintWriter(socketClient.getOutputStream(), true);
      Random incTs = new Random(10);
      long timestamp = 1460730000050L;
      double it = 0.12;
      int idx = 0;
      Double[] streamDemo = sigGen(100, 1);
      String message;
      while (true) {
        // construct the stream


        if (elems == 0) {
          message = timestamp + "," + Double.toString(it);
          System.out.println("genEvent() ?");
          String s = br.readLine();
          if (s.equals("quit()")) {
            socketClient.close();
            System.exit(0);
          }
          if (s.contains("-r")) {
            if (debugFlag.isEnableProfiling()) {
              GlobalFeatureProfiler.getInstance().start("srcthroughput");
            }
            counter = Integer.parseInt(s.split(" ")[1]);
            for (int id = 0; id < counter; id++) {
              message =
                  timestamp + "," + Double.toString(it) + "," + Double.toString(it + 0.53457) + ","
                      + dataStream[idx];
              out.println(message);
              // burst
              if (id % evtBurstSize == 0) {
                try {
                  Thread.sleep(evtBurstLatency);
                } catch (InterruptedException e) {
                  e.printStackTrace();
                }
              }
              idx++;
              if (idx == dataStream.length) {
                idx = 0;
              }
              it = it + 1.25;
              timestamp += incTs.nextInt(2);
            }
            if (debugFlag.isEnableProfiling()) {
              GlobalFeatureProfiler.getInstance().stop("srcthroughput");
              System.out.println("srcthr:" + counter * 1000.0 / GlobalFeatureProfiler.getInstance()
                  .toVal("srcthroughput").get(2));
            }
          }
          it = it + 1;
          out.println(message);
        } else {
          counter = elems;
          for (int id = 0; id < counter; id++) {
            message =
                timestamp + "," + Double.toString(it) + "," + Double.toString(it + 0.53457) + ","
                    + dataStream[idx];
            out.println(message);
            // burst
            if (id % evtBurstSize == 0) {
              try {
                Thread.sleep(evtBurstLatency);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            idx++;
            if (idx == dataStream.length) {
              idx = 0;
            }
            it = it + 1.25;
            timestamp += incTs.nextInt(2);
          }
          if (debugFlag.isEnableProfiling()) {
            GlobalFeatureProfiler.getInstance().stop("srcthroughput");
            System.out.println("srcthr:" + counter * 1000.0 / GlobalFeatureProfiler.getInstance()
                .toVal("srcthroughput").get(2));
          }
          if (out.checkError()) {
            return;
          }
          return;
        }
        if (out.checkError()) {
          return;
        }
        timestamp += 7200000;
      }
    } catch (IOException e) {
      e.printStackTrace();
      if (socketClient.isClosed()) {
        return;
      }
    } finally {
      try {
        socketClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
